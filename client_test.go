package squirreldb

import (
	"context"
	"testing"
	"time"
)

func TestClientErrors(t *testing.T) {
	t.Run("error constants are defined", func(t *testing.T) {
		if ErrNotConnected == nil {
			t.Error("ErrNotConnected should not be nil")
		}
		if ErrVersionMismatch == nil {
			t.Error("ErrVersionMismatch should not be nil")
		}
		if ErrAuthFailed == nil {
			t.Error("ErrAuthFailed should not be nil")
		}
		if ErrClosed == nil {
			t.Error("ErrClosed should not be nil")
		}
	})

	t.Run("error messages", func(t *testing.T) {
		if ErrNotConnected.Error() != "not connected" {
			t.Errorf("ErrNotConnected = %s, want 'not connected'", ErrNotConnected.Error())
		}
		if ErrVersionMismatch.Error() != "protocol version mismatch" {
			t.Errorf("ErrVersionMismatch = %s, want 'protocol version mismatch'", ErrVersionMismatch.Error())
		}
		if ErrAuthFailed.Error() != "authentication failed" {
			t.Errorf("ErrAuthFailed = %s, want 'authentication failed'", ErrAuthFailed.Error())
		}
		if ErrClosed.Error() != "connection closed" {
			t.Errorf("ErrClosed = %s, want 'connection closed'", ErrClosed.Error())
		}
	})
}

func TestConnectErrors(t *testing.T) {
	t.Run("connect to non-existent server", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := Connect(ctx, &Options{
			Host: "127.0.0.1",
			Port: 59999,
		})

		if err == nil {
			t.Error("Expected error when connecting to non-existent server")
		}
	})

	t.Run("connect with nil options uses defaults", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// This will fail to connect but should use default options
		_, err := Connect(ctx, nil)

		// Error expected since server isn't running
		if err == nil {
			t.Error("Expected connection error")
		}
	})

	t.Run("connect with invalid host", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := Connect(ctx, &Options{
			Host: "invalid.host.that.does.not.exist",
			Port: 8082,
		})

		if err == nil {
			t.Error("Expected error when connecting to invalid host")
		}
	})
}
