package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
	"github.com/mdp/qrterminal"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

type sqlite3Driver struct{ driver.Driver }

func init() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return
	}
	drv := db.Driver()
	db.Close()
	sql.Register("sqlite3", sqlite3Driver{Driver: drv})
}

// ─── MessageStore ────────────────────────────────────────────────────────────

type MessageStore struct {
	db *sql.DB
	mu sync.RWMutex
}

func NewMessageStore() (*MessageStore, error) {
	if err := os.MkdirAll("store", 0755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	db, err := sql.Open("sqlite3", "file:store/messages.db?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			PRIMARY KEY (id, chat_jid)
		);
		CREATE TABLE IF NOT EXISTS sent_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
			phone TEXT NOT NULL,
			message TEXT NOT NULL,
			message_id TEXT,
			success BOOLEAN NOT NULL,
			error TEXT
		);
	`)
	if err != nil {
		return nil, fmt.Errorf("create tables: %w", err)
	}
	return &MessageStore{db: db}, nil
}

func (s *MessageStore) StoreMessage(id, chatJID, sender, content string, ts time.Time, fromMe bool, mediaType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, media_type)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, chatJID, sender, content, ts, fromMe, mediaType)
	return err
}

func (s *MessageStore) LogSend(phone, message, messageID string, success bool, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.db.Exec(
		`INSERT INTO sent_messages (phone, message, message_id, success, error) VALUES (?, ?, ?, ?, ?)`,
		phone, message, messageID, success, errMsg,
	)
}

func (s *MessageStore) GetRecentMessages(limit int, afterDate, beforeDate, contact string) ([]map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit > 500 {
		limit = 500
	}
	query := "SELECT chat_jid, sender, content, timestamp, is_from_me, media_type FROM messages"
	args := []interface{}{}
	conditions := []string{}
	if afterDate != "" {
		conditions = append(conditions, "timestamp >= ?")
		args = append(args, afterDate+"T00:00:00Z")
	}
	if beforeDate != "" {
		conditions = append(conditions, "timestamp < ?")
		args = append(args, beforeDate+"T00:00:00Z")
	}
	if contact != "" {
		conditions = append(conditions, "(sender = ? OR chat_jid LIKE ?)")
		args = append(args, contact, contact+"%")
	}
	if len(conditions) > 0 {
		query += " WHERE " + conditions[0]
		for _, c := range conditions[1:] {
			query += " AND " + c
		}
	}
	query += " ORDER BY timestamp DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	msgs := []map[string]interface{}{}
	for rows.Next() {
		var chat, sender, content, media string
		var ts time.Time
		var fromMe bool
		if err := rows.Scan(&chat, &sender, &content, &ts, &fromMe, &media); err != nil {
			return nil, err
		}
		msgs = append(msgs, map[string]interface{}{
			"chat_jid":  chat,
			"sender":    sender,
			"content":   content,
			"timestamp": ts,
			"from_me":   fromMe,
			"media":     media,
		})
	}
	return msgs, nil
}

// ─── History sync ─────────────────────────────────────────────────────────────

func storeHistoryMessage(store *MessageStore, conversations []*waProto.Conversation) {
	for _, conv := range conversations {
		chatJID := conv.GetID()
		for _, histMsg := range conv.GetMessages() {
			info := histMsg.GetMessage()
			if info == nil {
				continue
			}
			msg := info.GetMessage()
			if msg == nil {
				continue
			}
			content := msg.GetConversation()
			if content == "" {
				if ext := msg.GetExtendedTextMessage(); ext != nil {
					content = ext.GetText()
				}
			}
			mediaType := ""
			if msg.GetImageMessage() != nil {
				mediaType = "image"
			}
			if msg.GetAudioMessage() != nil {
				mediaType = "audio"
			}
			fromMe := info.GetKey().GetFromMe()
			sender := info.GetKey().GetParticipant()
			if sender == "" {
				if fromMe {
					sender = "me"
				} else {
					sender = chatJID
				}
			}
			ts := time.Unix(int64(info.GetMessageTimestamp()), 0)
			store.StoreMessage(info.GetKey().GetID(), chatJID, sender, content, ts, fromMe, mediaType)
		}
	}
}

// ─── Send route types ────────────────────────────────────────────────────────

type SendRequest struct {
	To      string `json:"to"`
	Message string `json:"message"`
}

type SendResponse struct {
	Success   bool   `json:"success"`
	MessageID string `json:"message_id,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
	Error     string `json:"error,omitempty"`
}

// ─── Rate limiter (fenêtre glissante, 10 msg/min par destinataire) ───────────

type RateLimiter struct {
	mu       sync.Mutex
	requests map[string][]time.Time
	limit    int
	window   time.Duration
}

func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		requests: make(map[string][]time.Time),
		limit:    limit,
		window:   window,
	}
}

func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-rl.window)
	times := rl.requests[key]
	var valid []time.Time
	for _, t := range times {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}
	rl.requests[key] = valid
	if len(valid) >= rl.limit {
		return false
	}
	rl.requests[key] = append(valid, now)
	return true
}

var rateLimiter = NewRateLimiter(10, time.Minute)

var phoneRegex = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)

// ─── JSON helper ─────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v any, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	logger := waLog.Stdout("Client", "INFO", true)

	container, err := sqlstore.New(context.Background(), "sqlite3", "file:store/whatsapp.db?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", waLog.Stdout("Database", "INFO", true))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open whatsapp store: %v\n", err)
		os.Exit(1)
	}

	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get device: %v\n", err)
		os.Exit(1)
	}

	client := whatsmeow.NewClient(deviceStore, logger)

	messageStore, err := NewMessageStore()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open message store: %v\n", err)
		os.Exit(1)
	}
	defer messageStore.db.Close()

	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			content := v.Message.GetConversation()
			if content == "" {
				if ext := v.Message.GetExtendedTextMessage(); ext != nil {
					content = ext.GetText()
				}
			}
			mediaType := ""
			if v.Message.GetImageMessage() != nil {
				mediaType = "image"
			}
			if v.Message.GetAudioMessage() != nil {
				mediaType = "audio"
			}
			messageStore.StoreMessage(v.Info.ID, v.Info.Chat.String(), v.Info.Sender.User, content, v.Info.Timestamp, v.Info.IsFromMe, mediaType)
			fmt.Printf("New message from %s: %s\n", v.Info.Sender.User, content)
		case *events.HistorySync:
			storeHistoryMessage(messageStore, v.Data.GetConversations())
		}
	})

	internalKey := os.Getenv("INTERNAL_API_KEY")

	// GET /messages
	http.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != internalKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		limit := 50
		if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil {
			limit = l
		}
		msgs, err := messageStore.GetRecentMessages(limit, r.URL.Query().Get("after_date"), r.URL.Query().Get("before_date"), r.URL.Query().Get("contact"))
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(msgs)
	})

	// GET /contacts
	http.HandleFunc("/contacts", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != internalKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		contacts, err := client.Store.Contacts.GetAllContacts(context.Background())
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		result := map[string]string{}
		for jid, info := range contacts {
			name := info.FullName
			if name == "" {
				name = info.PushName
			}
			if name != "" {
				result[jid.User] = name
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	// GET /health
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})

	// POST /send
	http.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("X-Internal-Key") != internalKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		var req SendRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, SendResponse{Success: false, Error: "Invalid JSON"}, http.StatusBadRequest)
			return
		}
		if !phoneRegex.MatchString(req.To) {
			writeJSON(w, SendResponse{Success: false, Error: "Invalid phone number format (E.164 required, e.g. +33612345678)"}, http.StatusBadRequest)
			return
		}
		if req.Message == "" {
			writeJSON(w, SendResponse{Success: false, Error: "Message cannot be empty"}, http.StatusBadRequest)
			return
		}
		if !rateLimiter.Allow(req.To) {
			writeJSON(w, SendResponse{Success: false, Error: "Rate limit exceeded (10 msg/min per recipient)"}, http.StatusTooManyRequests)
			return
		}
		if client == nil || !client.IsConnected() {
			messageStore.LogSend(req.To, req.Message, "", false, "client disconnected")
			writeJSON(w, SendResponse{Success: false, Error: "WhatsApp client disconnected"}, http.StatusServiceUnavailable)
			return
		}

		jid := types.NewJID(strings.TrimPrefix(req.To, "+"), types.DefaultUserServer)
		msg := &waProto.Message{Conversation: proto.String(req.Message)}

		resp, err := client.SendMessage(context.Background(), jid, msg)
		if err != nil {
			messageStore.LogSend(req.To, req.Message, "", false, err.Error())
			writeJSON(w, SendResponse{Success: false, Error: err.Error()}, http.StatusInternalServerError)
			return
		}

		messageStore.LogSend(req.To, req.Message, resp.ID, true, "")
		writeJSON(w, SendResponse{
			Success:   true,
			MessageID: resp.ID,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		}, http.StatusOK)
	})

	srv := &http.Server{
		Addr:         ":8081",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		fmt.Println("Bridge API listening on :8081")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "HTTP server error: %v\n", err)
		}
	}()

	if client.Store.ID == nil {
		qrChan, err := client.GetQRChannel(context.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get QR channel: %v\n", err)
			os.Exit(1)
		}
		if err := client.Connect(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect: %v\n", err)
			os.Exit(1)
		}
		go func() {
			for qr := range qrChan {
				if qr.Event == "code" {
					qrterminal.GenerateHalfBlock(qr.Code, qrterminal.L, os.Stdout)
				}
			}
		}()
	} else {
		if err := client.Connect(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("Bridge running. POST /send now available.")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	client.Disconnect()
}
