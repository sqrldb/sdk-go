package squirreldb

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

var (
	ErrNotConnected    = errors.New("not connected")
	ErrVersionMismatch = errors.New("protocol version mismatch")
	ErrAuthFailed      = errors.New("authentication failed")
	ErrClosed          = errors.New("connection closed")
)

// Client is a SquirrelDB TCP client.
type Client struct {
	conn      net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer
	writeMu   sync.Mutex
	encoding  Encoding
	sessionID string
	requestID atomic.Uint64

	pending      map[string]chan *ServerMessage
	pendingMu    sync.RWMutex
	subscriptions map[string]chan *ChangeEvent
	subMu        sync.RWMutex

	closed   atomic.Bool
	closedCh chan struct{}
}

// Connect connects to a SquirrelDB server.
func Connect(ctx context.Context, opts *Options) (*Client, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	addr := fmt.Sprintf("%s:%d", opts.Host, opts.Port)

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	c := &Client{
		conn:          conn,
		reader:        bufio.NewReader(conn),
		writer:        bufio.NewWriter(conn),
		pending:       make(map[string]chan *ServerMessage),
		subscriptions: make(map[string]chan *ChangeEvent),
		closedCh:      make(chan struct{}),
	}

	// Perform handshake
	if err := c.handshake(opts); err != nil {
		conn.Close()
		return nil, err
	}

	// Start receive loop
	go c.receiveLoop()

	return c, nil
}

func (c *Client) handshake(opts *Options) error {
	flags := ProtocolFlags{
		MessagePack:  opts.UseMessagePack,
		JSONFallback: true,
	}

	// Send handshake
	handshake := BuildHandshake(opts.AuthToken, flags)
	if _, err := c.conn.Write(handshake); err != nil {
		return fmt.Errorf("failed to send handshake: %w", err)
	}

	// Read response (19 bytes)
	response := make([]byte, 19)
	if _, err := io.ReadFull(c.reader, response); err != nil {
		return fmt.Errorf("failed to read handshake response: %w", err)
	}

	resp, err := ParseHandshakeResponse(response)
	if err != nil {
		return err
	}

	switch resp.Status {
	case HandshakeSuccess:
		// OK
	case HandshakeVersionMismatch:
		return fmt.Errorf("%w: server=%d, client=%d", ErrVersionMismatch, resp.Version, ProtocolVersion)
	case HandshakeAuthFailed:
		return ErrAuthFailed
	default:
		return fmt.Errorf("unexpected handshake status: %d", resp.Status)
	}

	c.sessionID = UUIDToString(resp.SessionID)
	if resp.Flags.MessagePack {
		c.encoding = EncodingMessagePack
	} else {
		c.encoding = EncodingJSON
	}

	return nil
}

func (c *Client) receiveLoop() {
	defer close(c.closedCh)

	for !c.closed.Load() {
		// Read frame header (6 bytes)
		header := make([]byte, 6)
		if _, err := io.ReadFull(c.reader, header); err != nil {
			if !c.closed.Load() {
				c.handleDisconnect(err)
			}
			return
		}

		frameHeader, err := ParseFrameHeader(header)
		if err != nil {
			continue
		}

		if frameHeader.PayloadLength > MaxMessageSize {
			continue
		}

		// Read payload
		payload := make([]byte, frameHeader.PayloadLength)
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			if !c.closed.Load() {
				c.handleDisconnect(err)
			}
			return
		}

		// Decode message
		var msg ServerMessage
		if err := DecodeMessage(payload, frameHeader.Encoding, &msg); err != nil {
			continue
		}

		c.dispatchMessage(&msg)
	}
}

func (c *Client) dispatchMessage(msg *ServerMessage) {
	if msg.Type == "change" && msg.Change != nil {
		c.subMu.RLock()
		ch, ok := c.subscriptions[msg.ID]
		c.subMu.RUnlock()
		if ok {
			select {
			case ch <- msg.Change:
			default:
				// Channel full, drop message
			}
		}
		return
	}

	c.pendingMu.Lock()
	ch, ok := c.pending[msg.ID]
	if ok {
		delete(c.pending, msg.ID)
	}
	c.pendingMu.Unlock()

	if ok {
		ch <- msg
	}
}

func (c *Client) handleDisconnect(err error) {
	c.pendingMu.Lock()
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()
}

func (c *Client) nextID() string {
	return fmt.Sprintf("%d", c.requestID.Add(1))
}

func (c *Client) send(msg *ClientMessage) (*ServerMessage, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}

	payload, err := EncodeMessage(msg, c.encoding)
	if err != nil {
		return nil, fmt.Errorf("failed to encode message: %w", err)
	}

	frame := BuildFrame(MessageTypeRequest, c.encoding, payload)

	// Create response channel
	respCh := make(chan *ServerMessage, 1)
	c.pendingMu.Lock()
	c.pending[msg.ID] = respCh
	c.pendingMu.Unlock()

	// Send frame
	c.writeMu.Lock()
	_, err = c.conn.Write(frame)
	c.writeMu.Unlock()

	if err != nil {
		c.pendingMu.Lock()
		delete(c.pending, msg.ID)
		c.pendingMu.Unlock()
		return nil, fmt.Errorf("failed to send: %w", err)
	}

	// Wait for response
	select {
	case resp, ok := <-respCh:
		if !ok {
			return nil, ErrClosed
		}
		return resp, nil
	case <-c.closedCh:
		return nil, ErrClosed
	}
}

// SessionID returns the session ID.
func (c *Client) SessionID() string {
	return c.sessionID
}

// Query executes a query.
func (c *Client) Query(ctx context.Context, q string) (json.RawMessage, error) {
	msg := &ClientMessage{
		Type:  "query",
		ID:    c.nextID(),
		Query: q,
	}

	resp, err := c.send(msg)
	if err != nil {
		return nil, err
	}

	if resp.Type == "error" {
		return nil, errors.New(resp.Error)
	}

	return resp.Data, nil
}

// QueryTo executes a query and unmarshals the result into v.
func (c *Client) QueryTo(ctx context.Context, q string, v interface{}) error {
	data, err := c.Query(ctx, q)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// Insert inserts a document.
func (c *Client) Insert(ctx context.Context, collection string, data interface{}) (*Document, error) {
	msg := &ClientMessage{
		Type:       "insert",
		ID:         c.nextID(),
		Collection: collection,
		Data:       data,
	}

	resp, err := c.send(msg)
	if err != nil {
		return nil, err
	}

	if resp.Type == "error" {
		return nil, errors.New(resp.Error)
	}

	var doc Document
	if err := json.Unmarshal(resp.Data, &doc); err != nil {
		return nil, err
	}

	return &doc, nil
}

// Update updates a document.
func (c *Client) Update(ctx context.Context, collection, documentID string, data interface{}) (*Document, error) {
	msg := &ClientMessage{
		Type:       "update",
		ID:         c.nextID(),
		Collection: collection,
		DocumentID: documentID,
		Data:       data,
	}

	resp, err := c.send(msg)
	if err != nil {
		return nil, err
	}

	if resp.Type == "error" {
		return nil, errors.New(resp.Error)
	}

	var doc Document
	if err := json.Unmarshal(resp.Data, &doc); err != nil {
		return nil, err
	}

	return &doc, nil
}

// Delete deletes a document.
func (c *Client) Delete(ctx context.Context, collection, documentID string) (*Document, error) {
	msg := &ClientMessage{
		Type:       "delete",
		ID:         c.nextID(),
		Collection: collection,
		DocumentID: documentID,
	}

	resp, err := c.send(msg)
	if err != nil {
		return nil, err
	}

	if resp.Type == "error" {
		return nil, errors.New(resp.Error)
	}

	var doc Document
	if err := json.Unmarshal(resp.Data, &doc); err != nil {
		return nil, err
	}

	return &doc, nil
}

// ListCollections lists all collections.
func (c *Client) ListCollections(ctx context.Context) ([]string, error) {
	msg := &ClientMessage{
		Type: "listcollections",
		ID:   c.nextID(),
	}

	resp, err := c.send(msg)
	if err != nil {
		return nil, err
	}

	if resp.Type == "error" {
		return nil, errors.New(resp.Error)
	}

	var collections []string
	if err := json.Unmarshal(resp.Data, &collections); err != nil {
		return nil, err
	}

	return collections, nil
}

// Subscription represents an active subscription.
type Subscription struct {
	ID      string
	client  *Client
	changes chan *ChangeEvent
}

// Changes returns the channel for receiving change events.
func (s *Subscription) Changes() <-chan *ChangeEvent {
	return s.changes
}

// Unsubscribe unsubscribes from changes.
func (s *Subscription) Unsubscribe() error {
	s.client.subMu.Lock()
	delete(s.client.subscriptions, s.ID)
	s.client.subMu.Unlock()
	close(s.changes)

	// Send unsubscribe message
	msg := &ClientMessage{
		Type: "unsubscribe",
		ID:   s.ID,
	}

	payload, err := EncodeMessage(msg, s.client.encoding)
	if err != nil {
		return err
	}

	frame := BuildFrame(MessageTypeRequest, s.client.encoding, payload)

	s.client.writeMu.Lock()
	_, err = s.client.conn.Write(frame)
	s.client.writeMu.Unlock()

	return err
}

// Subscribe subscribes to changes.
func (c *Client) Subscribe(ctx context.Context, q string) (*Subscription, error) {
	id := c.nextID()
	msg := &ClientMessage{
		Type:  "subscribe",
		ID:    id,
		Query: q,
	}

	resp, err := c.send(msg)
	if err != nil {
		return nil, err
	}

	if resp.Type == "error" {
		return nil, errors.New(resp.Error)
	}

	changes := make(chan *ChangeEvent, 100)

	c.subMu.Lock()
	c.subscriptions[id] = changes
	c.subMu.Unlock()

	return &Subscription{
		ID:      id,
		client:  c,
		changes: changes,
	}, nil
}

// Ping pings the server.
func (c *Client) Ping(ctx context.Context) error {
	msg := &ClientMessage{
		Type: "ping",
		ID:   c.nextID(),
	}

	resp, err := c.send(msg)
	if err != nil {
		return err
	}

	if resp.Type != "pong" {
		return fmt.Errorf("unexpected response: %s", resp.Type)
	}

	return nil
}

// Close closes the connection.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil // Already closed
	}

	c.subMu.Lock()
	for id, ch := range c.subscriptions {
		close(ch)
		delete(c.subscriptions, id)
	}
	c.subMu.Unlock()

	return c.conn.Close()
}
