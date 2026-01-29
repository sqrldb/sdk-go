// Package squirreldb provides a Go client for SquirrelDB.
package squirreldb

import (
	"encoding/json"
	"time"
)

// Document represents a document stored in SquirrelDB.
type Document struct {
	ID         string          `json:"id" msgpack:"id"`
	Collection string          `json:"collection" msgpack:"collection"`
	Data       json.RawMessage `json:"data" msgpack:"data"`
	CreatedAt  time.Time       `json:"created_at" msgpack:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at" msgpack:"updated_at"`
}

// ChangeEvent represents a change event from a subscription.
type ChangeEvent struct {
	Type     string          `json:"type" msgpack:"type"`
	Document *Document       `json:"document,omitempty" msgpack:"document,omitempty"`
	New      *Document       `json:"new,omitempty" msgpack:"new,omitempty"`
	Old      json.RawMessage `json:"old,omitempty" msgpack:"old,omitempty"`
}

// ClientMessage is a message sent from client to server.
// Query can be either a string (legacy JS query) or a StructuredQuery object.
type ClientMessage struct {
	Type       string      `json:"type" msgpack:"type"`
	ID         string      `json:"id" msgpack:"id"`
	Query      interface{} `json:"query,omitempty" msgpack:"query,omitempty"`
	Collection string      `json:"collection,omitempty" msgpack:"collection,omitempty"`
	DocumentID string      `json:"document_id,omitempty" msgpack:"document_id,omitempty"`
	Data       interface{} `json:"data,omitempty" msgpack:"data,omitempty"`
}

// ServerMessage is a message sent from server to client.
type ServerMessage struct {
	Type   string          `json:"type" msgpack:"type"`
	ID     string          `json:"id" msgpack:"id"`
	Data   json.RawMessage `json:"data,omitempty" msgpack:"data,omitempty"`
	Change *ChangeEvent    `json:"change,omitempty" msgpack:"change,omitempty"`
	Error  string          `json:"error,omitempty" msgpack:"error,omitempty"`
}

// Options for connecting to SquirrelDB.
type Options struct {
	Host           string
	Port           int
	AuthToken      string
	UseMessagePack bool
}

// DefaultOptions returns default connection options.
func DefaultOptions() *Options {
	return &Options{
		Host:           "localhost",
		Port:           8082,
		AuthToken:      "",
		UseMessagePack: true,
	}
}
