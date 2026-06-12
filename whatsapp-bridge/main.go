package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type MessageStore struct {
	db  *sql.DB
	mu  sync.RWMutex
}

func NewMessageStore() (*MessageStore, error) {
	if err := os.MkdirAll("store", 0755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	db, err := sql.Open("sqlite3", "file:store/messages.db?_foreign_keys=on&_journal_mode=WAL")
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

func (s *MessageStore) GetRecentMessages(limit int) ([]map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`
		SELECT chat_jid, sender, content, timestamp, is_from_me, media_type
		FROM messages ORDER BY timestamp DESC LIMIT ?`, limit)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return msgs, nil
}

func main() {
	logger := waLog.Stdout("Client", "INFO", true)

	container, err := sqlstore.New("sqlite3", "file:store/whatsapp.db?_foreign_keys=on", waLog.Stdout("Database", "INFO", true))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open whatsapp store: %v\n", err)
		os.Exit(1)
	}

	deviceStore, err := container.GetFirstDevice()
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
		v, ok := evt.(*events.Message)
		if !ok {
			return
		}

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
			fmt.Fprintf(os.Stderr, "store message error: %v\n", err)
		}

		fmt.Printf("New message from %s: %s\n", v.Info.Sender.User, content)
	})

	internalKey := os.Getenv("INTERNAL_API_KEY")

	http.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != internalKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		msgs, err := messageStore.GetRecentMessages(50)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(msgs)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
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

	// Connexion WhatsApp après démarrage du HTTP server
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

	fmt.Println("Bridge running...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	client.Disconnect()
}
