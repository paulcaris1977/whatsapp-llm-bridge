// whatsapp-bridge/main.go — v1.3.0
// Changelog :
//   v1.0.0 — lecture messages + contacts + session persistée
//   v1.1.0 — POST /send avec rate limiting, logging SQLite, validation E.164
//   v1.2.0 — fix SQLITE_BUSY (WAL + busy_timeout 5s sur les deux bases)
//   v1.3.0 — GET /chats + enrichissement chat_name dans GET /messages + ChatCache thread-safe

package main

import (
	"context"
	"database/sql"
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

// ─── Validation ───────────────────────────────────────────────────────────────

// E.164 : +33612345678
var phoneRegex = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)

// JID groupe : 120363364799758719@g.us  ou  120363-1633559683@g.us (legacy)
var groupJIDRegex = regexp.MustCompile(`^\d+(-\d+)?@g\.us$`)

// ─── Models ───────────────────────────────────────────────────────────────────

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

// ─── ChatCache ────────────────────────────────────────────────────────────────

type ChatInfo struct {
	ChatJID string `json:"chat_jid"`
	Name    string `json:"name"`
	Type    string `json:"type"` // "group" ou "individual"
}

type ChatCache struct {
	mu    sync.RWMutex
	names map[string]string
}

func NewChatCache() *ChatCache {
	return &ChatCache{names: make(map[string]string)}
}

func (c *ChatCache) Get(jid string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	name, ok := c.names[jid]
	return name, ok
}

func (c *ChatCache) Set(jid, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.names[jid] = name
}

// ResolveName retourne le nom lisible d'un chat JID.
// Groupe (@g.us) → GetGroupInfo avec timeout 5s.
// Individuel → contacts store.
// Fallback → jidStr brut.
func (c *ChatCache) ResolveName(client *whatsmeow.Client, jidStr string) string {
	if name, ok := c.Get(jidStr); ok {
		return name
	}
	jid, err := types.ParseJID(jidStr)
	if err != nil {
		return jidStr
	}
	var name string
	if strings.HasSuffix(jidStr, "@g.us") {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		info, err := client.GetGroupInfo(ctx, jid)
		if err == nil && info != nil {
			name = info.Name
		}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		contacts, err := client.Store.Contacts.GetAllContacts(ctx)
		if err == nil {
			if info, ok := contacts[jid]; ok {
				name = info.FullName
				if name == "" {
					name = info.PushName
				}
			}
		}
	}
	if name == "" {
		name = jidStr
	}
	c.Set(jidStr, name)
	return name
}

// PopulateFromContacts pré-charge les noms des contacts individuels au démarrage.
func (c *ChatCache) PopulateFromContacts(client *whatsmeow.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	contacts, err := client.Store.Contacts.GetAllContacts(ctx)
	if err != nil {
		return
	}
	for jid, info := range contacts {
		name := info.FullName
		if name == "" {
			name = info.PushName
		}
		if name != "" {
			c.Set(jid.String(), name)
		}
	}
}

// ─── MessageStore ─────────────────────────────────────────────────────────────

type MessageStore struct {
	db *sql.DB
	mu sync.RWMutex
}

func NewMessageStore() (*MessageStore, error) {
	if err := os.MkdirAll("store", 0755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	db, err := sql.Open("sqlite", "file:store/messages.db?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open messages.db: %w", err)
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS messages (
			id         TEXT,
			chat_jid   TEXT,
			sender     TEXT,
			content    TEXT,
			timestamp  TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			PRIMARY KEY (id, chat_jid)
		);
		CREATE TABLE IF NOT EXISTS sent_messages (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp  DATETIME DEFAULT CURRENT_TIMESTAMP,
			recipient  TEXT    NOT NULL,
			message    TEXT    NOT NULL,
			message_id TEXT,
			success    BOOLEAN NOT NULL,
			error      TEXT
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

func (s *MessageStore) LogSend(recipient, message, messageID string, success bool, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.db.Exec(`
		INSERT INTO sent_messages (recipient, message, message_id, success, error)
		VALUES (?, ?, ?, ?, ?)`,
		recipient, message, messageID, success, errMsg)
}

// GetRecentMessages retourne les messages filtrés. Dates attendues au format YYYY-MM-DD.
// La normalisation du limit est effectuée ici uniquement.
func (s *MessageStore) GetRecentMessages(limit int, afterDate, beforeDate, contact string) ([]map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 || limit > 500 {
		limit = 50
	}

	query := "SELECT chat_jid, sender, content, timestamp, is_from_me, media_type FROM messages"
	args := []interface{}{}
	conditions := []string{}

	if afterDate != "" {
		if _, err := time.Parse("2006-01-02", afterDate); err != nil {
			return nil, fmt.Errorf("invalid after_date format (expected YYYY-MM-DD): %s", afterDate)
		}
		conditions = append(conditions, "timestamp >= ?")
		args = append(args, afterDate+"T00:00:00Z")
	}
	if beforeDate != "" {
		if _, err := time.Parse("2006-01-02", beforeDate); err != nil {
			return nil, fmt.Errorf("invalid before_date format (expected YYYY-MM-DD): %s", beforeDate)
		}
		conditions = append(conditions, "timestamp < ?")
		args = append(args, beforeDate+"T00:00:00Z")
	}
	if contact != "" {
		conditions = append(conditions, "(sender = ? OR chat_jid = ? OR chat_jid LIKE ?)")
		args = append(args, contact, contact, contact+"%")
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
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
	return msgs, rows.Err()
}

// ─── History sync ─────────────────────────────────────────────────────────────

// storeHistoryMessages est toujours appelé dans une goroutine dédiée (go storeHistoryMessages(...))
// pour ne pas bloquer l'event loop lors d'un HistorySync massif au premier démarrage.
func storeHistoryMessages(store *MessageStore, conversations []*waProto.Conversation) {
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
			} else if msg.GetAudioMessage() != nil {
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
			if err := store.StoreMessage(info.GetKey().GetID(), chatJID, sender, content, ts, fromMe, mediaType); err != nil {
				fmt.Fprintf(os.Stderr, "[history] store error for msg %s: %v\n", info.GetKey().GetID(), err)
			}
		}
	}
}

// ─── Rate Limiter (fenêtre glissante, sans fuite mémoire) ────────────────────

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

	// nil-safe : si key absente, times == nil, times[:0] == nil, range nil == no-op
	times := rl.requests[key]
	valid := times[:0]
	for _, t := range times {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}

	// Écrire la slice filtrée avant de décider (peut être nil — pas de fuite mémoire)
	rl.requests[key] = valid

	if len(valid) >= rl.limit {
		return false
	}
	// Lire depuis la map après écriture pour éviter toute ambiguïté avec valid
	rl.requests[key] = append(rl.requests[key], now)
	return true
}

var rateLimiter = NewRateLimiter(10, time.Minute)

// ─── Helpers ──────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v any, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Headers déjà envoyés — on ne peut que logger
		fmt.Fprintf(os.Stderr, "[http] writeJSON encode error: %v\n", err)
	}
}

// makeAuth accepte X-Internal-Key ou X-API-Key pour compatibilité avec
// le MCP server Python (X-Internal-Key) et d'autres clients (X-API-Key).
func makeAuth(key string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Internal-Key") != key && r.Header.Get("X-API-Key") != key {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next(w, r)
		}
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	internalKey := os.Getenv("INTERNAL_API_KEY")
	if internalKey == "" {
		fmt.Fprintln(os.Stderr, "INTERNAL_API_KEY environment variable is not set")
		os.Exit(1)
	}
	auth := makeAuth(internalKey)

	logger := waLog.Stdout("Client", "INFO", true)
	dbLogger := waLog.Stdout("Database", "INFO", true)

	container, err := sqlstore.New(
		context.Background(),
		"sqlite",
		"file:store/whatsapp.db?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)",
		dbLogger,
	)
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

	// Cache des noms de chats (groupes + contacts)
	chatCache := NewChatCache()

	// ── Event Handler ────────────────────────────────────────────────────────
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
			} else if v.Message.GetAudioMessage() != nil {
				mediaType = "audio"
			}
			if err := messageStore.StoreMessage(
				v.Info.ID,
				v.Info.Chat.String(),
				v.Info.Sender.User,
				content,
				v.Info.Timestamp,
				v.Info.IsFromMe,
				mediaType,
			); err != nil {
				fmt.Fprintf(os.Stderr, "[event] store message error: %v\n", err)
			}
			fmt.Printf("[msg] from=%s chat=%s content=%q\n", v.Info.Sender.User, v.Info.Chat.String(), content)

		case *events.HistorySync:
			// Goroutine dédiée pour ne pas bloquer l'event loop
			go storeHistoryMessages(messageStore, v.Data.GetConversations())

		case *events.Connected:
			fmt.Println("[wa] connected successfully")
			go chatCache.PopulateFromContacts(client)

		case *events.ConnectFailure:
			fmt.Fprintf(os.Stderr, "[wa] connect failure: %v\n", v.Reason)

		case *events.Disconnected:
			fmt.Println("[wa] disconnected")
		}
	})

	// ── HTTP Routes ───────────────────────────────────────────────────────────
	mux := http.NewServeMux()

	// /health — sans auth intentionnellement (Docker healthcheck, monitoring externe)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		connected := client != nil && client.IsConnected()
		writeJSON(w, map[string]interface{}{"status": "running", "connected": connected}, http.StatusOK)
	})

	// GET /chats
	mux.HandleFunc("/chats", auth(func(w http.ResponseWriter, r *http.Request) {
		// Peupler le cache depuis les contacts individuels
		chatCache.PopulateFromContacts(client)

		// Récupérer les JIDs distincts depuis la base messages
		rows, err := messageStore.db.Query(`SELECT DISTINCT chat_jid FROM messages ORDER BY chat_jid`)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var chats []ChatInfo
		for rows.Next() {
			var jidStr string
			if err := rows.Scan(&jidStr); err != nil {
				continue
			}
			typ := "individual"
			if strings.HasSuffix(jidStr, "@g.us") {
				typ = "group"
			}
			name := chatCache.ResolveName(client, jidStr)
			chats = append(chats, ChatInfo{ChatJID: jidStr, Name: name, Type: typ})
		}
		writeJSON(w, chats, http.StatusOK)
	}))

	// GET /messages
	mux.HandleFunc("/messages", auth(func(w http.ResponseWriter, r *http.Request) {
		// Parsing du limit dans le handler ; la normalisation reste dans GetRecentMessages
		limit := 50
		if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 {
			limit = l
		}
		msgs, err := messageStore.GetRecentMessages(
			limit,
			r.URL.Query().Get("after_date"),
			r.URL.Query().Get("before_date"),
			r.URL.Query().Get("contact"),
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Enrichir chaque message avec le nom lisible du chat
		for i := range msgs {
			jidStr, _ := msgs[i]["chat_jid"].(string)
			if jidStr != "" {
				msgs[i]["chat_name"] = chatCache.ResolveName(client, jidStr)
			}
		}
		writeJSON(w, msgs, http.StatusOK)
	}))

	// GET /contacts
	mux.HandleFunc("/contacts", auth(func(w http.ResponseWriter, r *http.Request) {
		contactsCtx, contactsCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer contactsCancel()
		contacts, err := client.Store.Contacts.GetAllContacts(contactsCtx)
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
		writeJSON(w, result, http.StatusOK)
	}))

	// POST /send — vérification méthode AVANT auth pour sémantique HTTP correcte
	mux.HandleFunc("/send", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		func(w http.ResponseWriter, r *http.Request) {
			var req SendRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeJSON(w, SendResponse{Success: false, Error: "Invalid JSON"}, http.StatusBadRequest)
				return
			}
			req.To = strings.TrimSpace(req.To)

			isPhone := phoneRegex.MatchString(req.To)
			isGroup := groupJIDRegex.MatchString(req.To)
			if !isPhone && !isGroup {
				writeJSON(w, SendResponse{
					Success: false,
					Error:   "Invalid recipient: use E.164 (+33612345678) or group JID (120363364799758719@g.us)",
				}, http.StatusBadRequest)
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

			if !client.IsConnected() {
				messageStore.LogSend(req.To, req.Message, "", false, "client disconnected")
				writeJSON(w, SendResponse{Success: false, Error: "WhatsApp client disconnected"}, http.StatusServiceUnavailable)
				return
			}

			// Construction du JID selon le type de destinataire
			var jid types.JID
			if isGroup {
				parsed, err := types.ParseJID(req.To)
				if err != nil {
					writeJSON(w, SendResponse{Success: false, Error: fmt.Sprintf("Invalid group JID: %v", err)}, http.StatusBadRequest)
					return
				}
				jid = parsed
			} else {
				jid = types.NewJID(strings.TrimPrefix(req.To, "+"), types.DefaultUserServer)
			}

			msg := &waProto.Message{Conversation: proto.String(req.Message)}
			sendCtx, sendCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer sendCancel()
			resp, err := client.SendMessage(sendCtx, jid, msg)
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
		}(w, r)
	}))

	// ── HTTP Server ───────────────────────────────────────────────────────────
	srv := &http.Server{
		Addr:         ":8081",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		fmt.Println("[http] Bridge API listening on :8081")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "[http] server error: %v\n", err)
		}
	}()

	// ── WhatsApp Connection ───────────────────────────────────────────────────
	if client.Store.ID == nil {
		// Premier démarrage : afficher le QR code dans une goroutine non bloquante
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
				switch qr.Event {
				case "code":
					fmt.Println("\n[wa] Scan this QR code with WhatsApp:")
					qrterminal.GenerateHalfBlock(qr.Code, qrterminal.L, os.Stdout)
				case "success":
					fmt.Println("[wa] QR scanned successfully!")
					return
				default:
					fmt.Printf("[wa] QR event: %s\n", qr.Event)
				}
			}
		}()
	} else {
		if err := client.Connect(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("[bridge] ✅ Running on :8081")

	// ── Graceful Shutdown ─────────────────────────────────────────────────────
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("[bridge] Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	client.Disconnect()
	fmt.Println("[bridge] Bye.")
}
