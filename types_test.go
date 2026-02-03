// SquirrelDB Go SDK - Types Tests

package squirreldb

import (
	"encoding/json"
	"testing"
)

func TestDocumentFromJSON(t *testing.T) {
	data := `{
		"id": "123",
		"collection": "users",
		"data": {"name": "Test"},
		"created_at": "2024-01-01T00:00:00Z",
		"updated_at": "2024-01-01T00:00:00Z"
	}`

	var doc Document
	err := json.Unmarshal([]byte(data), &doc)
	if err != nil {
		t.Fatalf("Failed to unmarshal document: %v", err)
	}

	if doc.Id != "123" {
		t.Errorf("Expected id '123', got '%s'", doc.Id)
	}
	if doc.Collection != "users" {
		t.Errorf("Expected collection 'users', got '%s'", doc.Collection)
	}
	if doc.Data["name"] != "Test" {
		t.Errorf("Expected data.name 'Test', got '%v'", doc.Data["name"])
	}
	if doc.CreatedAt != "2024-01-01T00:00:00Z" {
		t.Errorf("Expected created_at '2024-01-01T00:00:00Z', got '%s'", doc.CreatedAt)
	}
}

func TestDocumentToJSON(t *testing.T) {
	doc := Document{
		Id:         "test-id",
		Collection: "test-collection",
		Data:       map[string]interface{}{"foo": "bar"},
		CreatedAt:  "2024-01-01T00:00:00Z",
		UpdatedAt:  "2024-01-01T00:00:00Z",
	}

	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Failed to marshal document: %v", err)
	}

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Failed to unmarshal result: %v", err)
	}

	if result["id"] != "test-id" {
		t.Errorf("Expected id 'test-id', got '%v'", result["id"])
	}
}

func TestChangeEventInitial(t *testing.T) {
	data := `{
		"type": "initial",
		"document": {
			"id": "123",
			"collection": "users",
			"data": {"name": "Test"},
			"created_at": "2024-01-01T00:00:00Z",
			"updated_at": "2024-01-01T00:00:00Z"
		}
	}`

	var event ChangeEvent
	err := json.Unmarshal([]byte(data), &event)
	if err != nil {
		t.Fatalf("Failed to unmarshal change event: %v", err)
	}

	if event.Type != ChangeTypeInitial {
		t.Errorf("Expected type 'initial', got '%s'", event.Type)
	}
	if event.Document == nil {
		t.Error("Expected document to be non-nil")
	}
	if event.Document.Id != "123" {
		t.Errorf("Expected document.id '123', got '%s'", event.Document.Id)
	}
}

func TestChangeEventInsert(t *testing.T) {
	data := `{
		"type": "insert",
		"new": {
			"id": "123",
			"collection": "users",
			"data": {"name": "Test"},
			"created_at": "2024-01-01T00:00:00Z",
			"updated_at": "2024-01-01T00:00:00Z"
		}
	}`

	var event ChangeEvent
	err := json.Unmarshal([]byte(data), &event)
	if err != nil {
		t.Fatalf("Failed to unmarshal change event: %v", err)
	}

	if event.Type != ChangeTypeInsert {
		t.Errorf("Expected type 'insert', got '%s'", event.Type)
	}
	if event.New == nil {
		t.Error("Expected new to be non-nil")
	}
}

func TestChangeEventUpdate(t *testing.T) {
	event := ChangeEvent{
		Type: ChangeTypeUpdate,
	}

	if event.Type != "update" {
		t.Errorf("Expected type 'update', got '%s'", event.Type)
	}
}

func TestChangeEventDelete(t *testing.T) {
	event := ChangeEvent{
		Type: ChangeTypeDelete,
	}

	if event.Type != "delete" {
		t.Errorf("Expected type 'delete', got '%s'", event.Type)
	}
}

func TestBucketStructure(t *testing.T) {
	bucket := Bucket{
		Name:      "my-bucket",
		CreatedAt: "2024-01-01T00:00:00Z",
	}

	if bucket.Name != "my-bucket" {
		t.Errorf("Expected name 'my-bucket', got '%s'", bucket.Name)
	}
	if bucket.CreatedAt != "2024-01-01T00:00:00Z" {
		t.Errorf("Expected created_at '2024-01-01T00:00:00Z', got '%s'", bucket.CreatedAt)
	}
}

func TestStorageObjectStructure(t *testing.T) {
	contentType := "text/plain"
	obj := StorageObject{
		Key:          "path/to/file.txt",
		Size:         1024,
		Etag:         "d41d8cd98f00b204e9800998ecf8427e",
		LastModified: "2024-01-01T00:00:00Z",
		ContentType:  &contentType,
	}

	if obj.Key != "path/to/file.txt" {
		t.Errorf("Expected key 'path/to/file.txt', got '%s'", obj.Key)
	}
	if obj.Size != 1024 {
		t.Errorf("Expected size 1024, got %d", obj.Size)
	}
	if *obj.ContentType != "text/plain" {
		t.Errorf("Expected content_type 'text/plain', got '%s'", *obj.ContentType)
	}
}

func TestStorageObjectNullContentType(t *testing.T) {
	obj := StorageObject{
		Key:         "file.bin",
		Size:        2048,
		Etag:        "abc123",
		ContentType: nil,
	}

	if obj.ContentType != nil {
		t.Error("Expected content_type to be nil")
	}
}
