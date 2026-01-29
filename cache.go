package squirreldb

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Cache client errors
var (
	ErrCacheNotConnected = errors.New("cache not connected")
	ErrCacheClosed       = errors.New("cache connection closed")
	ErrKeyNotFound       = errors.New("key not found")
)

// CacheClient is a Redis-compatible cache client using RESP protocol
type CacheClient struct {
	conn    net.Conn
	reader  *bufio.Reader
	writeMu sync.Mutex
	readMu  sync.Mutex
	closed  atomic.Bool
}

// CacheOptions configures the cache client connection
type CacheOptions struct {
	Host string
	Port int
}

// DefaultCacheOptions returns default cache connection options
func DefaultCacheOptions() *CacheOptions {
	return &CacheOptions{
		Host: "localhost",
		Port: 6379,
	}
}

// ConnectCache connects to a Redis-compatible cache server
func ConnectCache(ctx context.Context, opts *CacheOptions) (*CacheClient, error) {
	if opts == nil {
		opts = DefaultCacheOptions()
	}

	if opts.Host == "" {
		opts.Host = "localhost"
	}
	if opts.Port == 0 {
		opts.Port = 6379
	}

	addr := fmt.Sprintf("%s:%d", opts.Host, opts.Port)

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to cache: %w", err)
	}

	c := &CacheClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}

	return c, nil
}

// execute sends a command and reads the response
func (c *CacheClient) execute(ctx context.Context, args ...string) (RespValue, error) {
	if c.closed.Load() {
		return RespValue{}, ErrCacheClosed
	}

	cmd := encodeCommand(args...)

	// Handle context deadline
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetDeadline(deadline); err != nil {
			return RespValue{}, fmt.Errorf("set deadline: %w", err)
		}
		defer c.conn.SetDeadline(time.Time{})
	}

	// Write command
	c.writeMu.Lock()
	_, err := c.conn.Write(cmd)
	c.writeMu.Unlock()

	if err != nil {
		return RespValue{}, fmt.Errorf("write command: %w", err)
	}

	// Read response
	c.readMu.Lock()
	resp, err := readResp(c.reader)
	c.readMu.Unlock()

	if err != nil {
		return RespValue{}, fmt.Errorf("read response: %w", err)
	}

	return resp, nil
}

// Get retrieves a value by key
func (c *CacheClient) Get(ctx context.Context, key string) (string, error) {
	resp, err := c.execute(ctx, "GET", key)
	if err != nil {
		return "", err
	}

	if resp.IsNull {
		return "", ErrKeyNotFound
	}

	return resp.asString()
}

// Set stores a key-value pair with optional TTL
func (c *CacheClient) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	var resp RespValue
	var err error

	if ttl > 0 {
		ms := ttl.Milliseconds()
		resp, err = c.execute(ctx, "SET", key, value, "PX", strconv.FormatInt(ms, 10))
	} else {
		resp, err = c.execute(ctx, "SET", key, value)
	}

	if err != nil {
		return err
	}

	return resp.asOK()
}

// Del deletes a key and returns true if it existed
func (c *CacheClient) Del(ctx context.Context, key string) (bool, error) {
	resp, err := c.execute(ctx, "DEL", key)
	if err != nil {
		return false, err
	}

	return resp.asBool()
}

// Exists checks if a key exists
func (c *CacheClient) Exists(ctx context.Context, key string) (bool, error) {
	resp, err := c.execute(ctx, "EXISTS", key)
	if err != nil {
		return false, err
	}

	return resp.asBool()
}

// Expire sets TTL on an existing key
func (c *CacheClient) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	secs := int64(ttl.Seconds())
	resp, err := c.execute(ctx, "EXPIRE", key, strconv.FormatInt(secs, 10))
	if err != nil {
		return false, err
	}

	return resp.asBool()
}

// TTL returns the remaining TTL of a key
// Returns -2 if key does not exist, -1 if key has no TTL
func (c *CacheClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	resp, err := c.execute(ctx, "TTL", key)
	if err != nil {
		return 0, err
	}

	secs, err := resp.asInt()
	if err != nil {
		return 0, err
	}

	if secs < 0 {
		return time.Duration(secs), nil
	}

	return time.Duration(secs) * time.Second, nil
}

// Persist removes TTL from a key
func (c *CacheClient) Persist(ctx context.Context, key string) (bool, error) {
	resp, err := c.execute(ctx, "PERSIST", key)
	if err != nil {
		return false, err
	}

	return resp.asBool()
}

// Incr increments a key by 1
func (c *CacheClient) Incr(ctx context.Context, key string) (int64, error) {
	resp, err := c.execute(ctx, "INCR", key)
	if err != nil {
		return 0, err
	}

	return resp.asInt()
}

// Decr decrements a key by 1
func (c *CacheClient) Decr(ctx context.Context, key string) (int64, error) {
	resp, err := c.execute(ctx, "DECR", key)
	if err != nil {
		return 0, err
	}

	return resp.asInt()
}

// IncrBy increments a key by the specified amount
func (c *CacheClient) IncrBy(ctx context.Context, key string, amount int64) (int64, error) {
	resp, err := c.execute(ctx, "INCRBY", key, strconv.FormatInt(amount, 10))
	if err != nil {
		return 0, err
	}

	return resp.asInt()
}

// Keys returns all keys matching the pattern
func (c *CacheClient) Keys(ctx context.Context, pattern string) ([]string, error) {
	resp, err := c.execute(ctx, "KEYS", pattern)
	if err != nil {
		return nil, err
	}

	return resp.asStringSlice()
}

// MGet retrieves multiple values by keys
// Returns empty string for keys that don't exist
func (c *CacheClient) MGet(ctx context.Context, keys ...string) ([]string, error) {
	args := make([]string, len(keys)+1)
	args[0] = "MGET"
	copy(args[1:], keys)

	resp, err := c.execute(ctx, args...)
	if err != nil {
		return nil, err
	}

	values, _, err := resp.asNullableStringSlice()
	return values, err
}

// MSet sets multiple key-value pairs
func (c *CacheClient) MSet(ctx context.Context, pairs map[string]string) error {
	args := make([]string, 1+len(pairs)*2)
	args[0] = "MSET"
	i := 1
	for k, v := range pairs {
		args[i] = k
		args[i+1] = v
		i += 2
	}

	resp, err := c.execute(ctx, args...)
	if err != nil {
		return err
	}

	return resp.asOK()
}

// DBSize returns the number of keys in the database
func (c *CacheClient) DBSize(ctx context.Context) (int64, error) {
	resp, err := c.execute(ctx, "DBSIZE")
	if err != nil {
		return 0, err
	}

	return resp.asInt()
}

// Flush removes all keys from the current database
func (c *CacheClient) Flush(ctx context.Context) error {
	resp, err := c.execute(ctx, "FLUSHDB")
	if err != nil {
		return err
	}

	return resp.asOK()
}

// Info returns server information as a map
func (c *CacheClient) Info(ctx context.Context) (map[string]interface{}, error) {
	resp, err := c.execute(ctx, "INFO")
	if err != nil {
		return nil, err
	}

	str, err := resp.asString()
	if err != nil {
		return nil, err
	}

	return parseInfo(str), nil
}

// parseInfo parses Redis INFO response into a map
func parseInfo(s string) map[string]interface{} {
	result := make(map[string]interface{})
	var currentSection string

	lines := strings.Split(s, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "#") {
			currentSection = strings.TrimSpace(strings.TrimPrefix(line, "#"))
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := parts[0]
		value := parts[1]

		if currentSection != "" {
			key = currentSection + "_" + key
		}

		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			result[key] = intVal
		} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			result[key] = floatVal
		} else {
			result[key] = value
		}
	}

	return result
}

// Ping checks if the server is responding
func (c *CacheClient) Ping(ctx context.Context) error {
	resp, err := c.execute(ctx, "PING")
	if err != nil {
		return err
	}

	s, err := resp.asString()
	if err != nil {
		return err
	}

	if s != "PONG" {
		return fmt.Errorf("unexpected PING response: %s", s)
	}

	return nil
}

// Close closes the connection
func (c *CacheClient) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	return c.conn.Close()
}
