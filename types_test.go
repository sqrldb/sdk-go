package squirreldb

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	if opts.Host != "localhost" {
		t.Errorf("Host = %s, want localhost", opts.Host)
	}
	if opts.Port != 8082 {
		t.Errorf("Port = %d, want 8082", opts.Port)
	}
	if opts.AuthToken != "" {
		t.Errorf("AuthToken = %s, want empty string", opts.AuthToken)
	}
	if !opts.UseMessagePack {
		t.Error("UseMessagePack should be true by default")
	}
}

func TestOptions(t *testing.T) {
	t.Run("custom options", func(t *testing.T) {
		opts := &Options{
			Host:           "db.example.com",
			Port:           9000,
			AuthToken:      "my-token",
			UseMessagePack: false,
		}

		if opts.Host != "db.example.com" {
			t.Errorf("Host = %s, want db.example.com", opts.Host)
		}
		if opts.Port != 9000 {
			t.Errorf("Port = %d, want 9000", opts.Port)
		}
		if opts.AuthToken != "my-token" {
			t.Errorf("AuthToken = %s, want my-token", opts.AuthToken)
		}
		if opts.UseMessagePack {
			t.Error("UseMessagePack should be false")
		}
	})
}

func TestDocumentSerialization(t *testing.T) {
	t.Run("JSON marshal", func(t *testing.T) {
		doc := Document{
			ID:         "test-id",
			Collection: "users",
			Data:       json.RawMessage(`{"name":"Alice"}`),
			CreatedAt:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			UpdatedAt:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		}

		data, err := json.Marshal(doc)
		if err != nil {
			t.Fatalf("Marshal() error = %v", err)
		}

		var decoded Document
		err = json.Unmarshal(data, &decoded)
		if err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if decoded.ID != doc.ID {
			t.Errorf("ID = %s, want %s", decoded.ID, doc.ID)
		}
		if decoded.Collection != doc.Collection {
			t.Errorf("Collection = %s, want %s", decoded.Collection, doc.Collection)
		}
	})

	t.Run("JSON unmarshal", func(t *testing.T) {
		jsonStr := `{
			"id": "doc-123",
			"collection": "users",
			"data": {"name": "Bob", "age": 30},
			"created_at": "2024-01-01T00:00:00Z",
			"updated_at": "2024-01-02T00:00:00Z"
		}`

		var doc Document
		err := json.Unmarshal([]byte(jsonStr), &doc)
		if err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if doc.ID != "doc-123" {
			t.Errorf("ID = %s, want doc-123", doc.ID)
		}
		if doc.Collection != "users" {
			t.Errorf("Collection = %s, want users", doc.Collection)
		}
		if doc.Data == nil {
			t.Error("Data should not be nil")
		}
	})
}

func TestChangeEventSerialization(t *testing.T) {
	t.Run("initial event", func(t *testing.T) {
		jsonStr := `{
			"type": "initial",
			"document": {
				"id": "doc-1",
				"collection": "users",
				"data": {"name": "Test"},
				"created_at": "2024-01-01T00:00:00Z",
				"updated_at": "2024-01-01T00:00:00Z"
			}
		}`

		var event ChangeEvent
		err := json.Unmarshal([]byte(jsonStr), &event)
		if err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if event.Type != "initial" {
			t.Errorf("Type = %s, want initial", event.Type)
		}
		if event.Document == nil {
			t.Error("Document should not be nil")
		}
		if event.Document.ID != "doc-1" {
			t.Errorf("Document.ID = %s, want doc-1", event.Document.ID)
		}
	})

	t.Run("insert event", func(t *testing.T) {
		jsonStr := `{
			"type": "insert",
			"new": {
				"id": "doc-2",
				"collection": "users",
				"data": {"name": "New"},
				"created_at": "2024-01-01T00:00:00Z",
				"updated_at": "2024-01-01T00:00:00Z"
			}
		}`

		var event ChangeEvent
		err := json.Unmarshal([]byte(jsonStr), &event)
		if err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if event.Type != "insert" {
			t.Errorf("Type = %s, want insert", event.Type)
		}
		if event.New == nil {
			t.Error("New should not be nil")
		}
	})

	t.Run("update event", func(t *testing.T) {
		jsonStr := `{
			"type": "update",
			"old": {"name": "Old"},
			"new": {
				"id": "doc-3",
				"collection": "users",
				"data": {"name": "Updated"},
				"created_at": "2024-01-01T00:00:00Z",
				"updated_at": "2024-01-02T00:00:00Z"
			}
		}`

		var event ChangeEvent
		err := json.Unmarshal([]byte(jsonStr), &event)
		if err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if event.Type != "update" {
			t.Errorf("Type = %s, want update", event.Type)
		}
		if event.New == nil {
			t.Error("New should not be nil")
		}
		if event.Old == nil {
			t.Error("Old should not be nil")
		}
	})

	t.Run("delete event", func(t *testing.T) {
		jsonStr := `{
			"type": "delete",
			"old": {
				"id": "doc-4",
				"collection": "users",
				"data": {"name": "Deleted"},
				"created_at": "2024-01-01T00:00:00Z",
				"updated_at": "2024-01-01T00:00:00Z"
			}
		}`

		var event ChangeEvent
		err := json.Unmarshal([]byte(jsonStr), &event)
		if err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if event.Type != "delete" {
			t.Errorf("Type = %s, want delete", event.Type)
		}
	})
}
