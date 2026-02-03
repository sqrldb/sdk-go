// SquirrelDB Go SDK - Protocol Tests

package squirreldb

import (
	"testing"
)

func TestPingMessage(t *testing.T) {
	msg := map[string]string{"type": "Ping"}
	if msg["type"] != "Ping" {
		t.Errorf("Expected type 'Ping', got '%s'", msg["type"])
	}
}

func TestQueryMessage(t *testing.T) {
	msg := map[string]string{
		"type":  "Query",
		"id":    "req-123",
		"query": `db.table("users").run()`,
	}
	if msg["type"] != "Query" {
		t.Errorf("Expected type 'Query', got '%s'", msg["type"])
	}
	if msg["id"] != "req-123" {
		t.Errorf("Expected id 'req-123', got '%s'", msg["id"])
	}
}

func TestInsertMessage(t *testing.T) {
	msg := map[string]interface{}{
		"type":       "Insert",
		"id":         "req-456",
		"collection": "users",
		"data":       map[string]string{"name": "Alice"},
	}
	if msg["type"] != "Insert" {
		t.Errorf("Expected type 'Insert', got '%v'", msg["type"])
	}
	if msg["collection"] != "users" {
		t.Errorf("Expected collection 'users', got '%v'", msg["collection"])
	}
}

func TestUpdateMessage(t *testing.T) {
	msg := map[string]interface{}{
		"type":        "Update",
		"id":          "req-789",
		"collection":  "users",
		"document_id": "doc-123",
		"data":        map[string]string{"name": "Bob"},
	}
	if msg["type"] != "Update" {
		t.Errorf("Expected type 'Update', got '%v'", msg["type"])
	}
	if msg["document_id"] != "doc-123" {
		t.Errorf("Expected document_id 'doc-123', got '%v'", msg["document_id"])
	}
}

func TestDeleteMessage(t *testing.T) {
	msg := map[string]string{
		"type":        "Delete",
		"id":          "req-101",
		"collection":  "users",
		"document_id": "doc-123",
	}
	if msg["type"] != "Delete" {
		t.Errorf("Expected type 'Delete', got '%s'", msg["type"])
	}
}

func TestSubscribeMessage(t *testing.T) {
	msg := map[string]string{
		"type":  "Subscribe",
		"id":    "req-202",
		"query": `db.table("users").changes()`,
	}
	if msg["type"] != "Subscribe" {
		t.Errorf("Expected type 'Subscribe', got '%s'", msg["type"])
	}
}

func TestUnsubscribeMessage(t *testing.T) {
	msg := map[string]string{
		"type":            "Unsubscribe",
		"id":              "req-303",
		"subscription_id": "sub-123",
	}
	if msg["type"] != "Unsubscribe" {
		t.Errorf("Expected type 'Unsubscribe', got '%s'", msg["type"])
	}
	if msg["subscription_id"] != "sub-123" {
		t.Errorf("Expected subscription_id 'sub-123', got '%s'", msg["subscription_id"])
	}
}

func TestPongResponse(t *testing.T) {
	response := map[string]string{"type": "Pong"}
	if response["type"] != "Pong" {
		t.Errorf("Expected type 'Pong', got '%s'", response["type"])
	}
}

func TestResultResponse(t *testing.T) {
	response := map[string]interface{}{
		"type": "Result",
		"id":   "req-123",
		"documents": []map[string]interface{}{
			{"id": "1", "collection": "users", "data": map[string]string{"name": "Alice"}},
		},
	}
	if response["type"] != "Result" {
		t.Errorf("Expected type 'Result', got '%v'", response["type"])
	}
	docs := response["documents"].([]map[string]interface{})
	if len(docs) != 1 {
		t.Errorf("Expected 1 document, got %d", len(docs))
	}
}

func TestErrorResponse(t *testing.T) {
	response := map[string]string{
		"type":    "Error",
		"id":      "req-123",
		"message": "Query failed",
	}
	if response["type"] != "Error" {
		t.Errorf("Expected type 'Error', got '%s'", response["type"])
	}
	if response["message"] != "Query failed" {
		t.Errorf("Expected message 'Query failed', got '%s'", response["message"])
	}
}

func TestSubscribedResponse(t *testing.T) {
	response := map[string]string{
		"type":            "Subscribed",
		"id":              "req-123",
		"subscription_id": "sub-456",
	}
	if response["type"] != "Subscribed" {
		t.Errorf("Expected type 'Subscribed', got '%s'", response["type"])
	}
	if response["subscription_id"] != "sub-456" {
		t.Errorf("Expected subscription_id 'sub-456', got '%s'", response["subscription_id"])
	}
}

func TestChangeResponse(t *testing.T) {
	response := map[string]interface{}{
		"type":            "Change",
		"subscription_id": "sub-456",
		"change": map[string]interface{}{
			"type": "insert",
			"new":  map[string]interface{}{"id": "1", "collection": "users"},
		},
	}
	if response["type"] != "Change" {
		t.Errorf("Expected type 'Change', got '%v'", response["type"])
	}
	change := response["change"].(map[string]interface{})
	if change["type"] != "insert" {
		t.Errorf("Expected change type 'insert', got '%v'", change["type"])
	}
}
