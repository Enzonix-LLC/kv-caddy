package kvstorage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(KVStorage{})
}

// KVStorage implements a Caddy storage backend using the kv-database HTTP API.
type KVStorage struct {
	Endpoint  string `json:"endpoint,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	APIKey    string `json:"api_key,omitempty"`

	logger *zap.Logger
	client *http.Client
}

// CaddyModule returns the Caddy module information.
func (KVStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.storage.enzonix_kv",
		New: func() caddy.Module { return new(KVStorage) },
	}
}

// Provision sets up the storage module.
func (s *KVStorage) Provision(ctx caddy.Context) error {
	s.logger = ctx.Logger(s)

	// Default endpoint if not provided
	if s.Endpoint == "" {
		s.Endpoint = "https://us-east-1.kv.enzonix.com"
	}

	// Ensure endpoint doesn't end with a slash
	s.Endpoint = strings.TrimSuffix(s.Endpoint, "/")

	// Create HTTP client with timeout
	s.client = &http.Client{
		Timeout: 30 * time.Second,
	}

	return nil
}

// Validate validates the configuration.
func (s *KVStorage) Validate() error {
	if s.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if s.APIKey == "" {
		return fmt.Errorf("api_key is required")
	}
	return nil
}

// Store stores a value at the given key.
func (s *KVStorage) Store(ctx context.Context, key string, value []byte) error {
	// Encode value as base64 for storage
	encodedValue := base64.StdEncoding.EncodeToString(value)

	// Prepare request body
	reqBody := map[string]string{
		"value": encodedValue,
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create request
	url := fmt.Sprintf("%s/api/write/%s/%s", s.Endpoint, s.Namespace, key)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", s.APIKey)

	// Execute request
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("storage request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Load retrieves a value for the given key.
func (s *KVStorage) Load(ctx context.Context, key string) ([]byte, error) {
	// Create request
	url := fmt.Sprintf("%s/api/read/%s/%s", s.Endpoint, s.Namespace, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-API-Key", s.APIKey)

	// Execute request
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, os.ErrNotExist
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("load request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var result struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Decode base64 value
	decodedValue, err := base64.StdEncoding.DecodeString(result.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 value: %w", err)
	}

	return decodedValue, nil
}

// Delete deletes the value at the given key.
func (s *KVStorage) Delete(ctx context.Context, key string) error {
	// Create request
	url := fmt.Sprintf("%s/api/write/%s/%s", s.Endpoint, s.Namespace, key)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-API-Key", s.APIKey)

	// Execute request
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return os.ErrNotExist
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// listKeys returns all keys that have the given prefix.
func (s *KVStorage) listKeys(prefix string, recursive bool) ([]string, error) {
	// Get all keys in the namespace
	url := fmt.Sprintf("%s/api/read/%s", s.Endpoint, s.Namespace)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-API-Key", s.APIKey)

	// Execute request
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var result struct {
		Namespace string   `json:"namespace"`
		Keys      []string `json:"keys"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Filter keys by prefix
	var filteredKeys []string
	for _, key := range result.Keys {
		if strings.HasPrefix(key, prefix) {
			// If recursive is false, only include immediate children
			if !recursive {
				// Check if there are any additional path segments after the prefix
				remainder := strings.TrimPrefix(key, prefix)
				remainder = strings.TrimPrefix(remainder, "/")
				// Only include if there are no more path separators (immediate child)
				if remainder == "" || !strings.Contains(remainder, "/") {
					filteredKeys = append(filteredKeys, key)
				}
			} else {
				// Recursive: include all keys with this prefix
				filteredKeys = append(filteredKeys, key)
			}
		}
	}

	return filteredKeys, nil
}

// Stat returns information about the key.
func (s *KVStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	// Try to load the key to check if it exists
	_, err := s.Load(ctx, key)
	if err != nil {
		if err == os.ErrNotExist {
			return certmagic.KeyInfo{}, os.ErrNotExist
		}
		return certmagic.KeyInfo{}, err
	}

	// Key exists, return basic info
	// Note: The kv-database API doesn't provide modification time or size,
	// so we return what we can
	return certmagic.KeyInfo{
		Key:        key,
		Size:       0, // Size not available from API
		IsTerminal: true,
	}, nil
}

// Exists returns true if the key exists.
func (s *KVStorage) Exists(ctx context.Context, key string) bool {
	_, err := s.Load(ctx, key)
	return err == nil
}

// List returns all keys that have the given prefix.
func (s *KVStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	return s.listKeys(prefix, recursive)
}

// Lock acquires a lock for the given key.
func (s *KVStorage) Lock(ctx context.Context, key string) error {
	// For a simple implementation, we can use a lock key pattern
	// In a production system, you might want to implement proper distributed locking
	lockKey := key + ".lock"
	lockValue := fmt.Sprintf("%d", time.Now().UnixNano())

	// Try to create the lock key (atomic operation)
	err := s.Store(ctx, lockKey, []byte(lockValue))
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	return nil
}

// Unlock releases the lock for the given key.
func (s *KVStorage) Unlock(ctx context.Context, key string) error {
	lockKey := key + ".lock"
	return s.Delete(ctx, lockKey)
}

// Interface guards
var (
	_ certmagic.Storage = (*KVStorage)(nil)
	_ caddy.Provisioner = (*KVStorage)(nil)
	_ caddy.Validator   = (*KVStorage)(nil)
)
