package kvstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
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

// CertMagicStorage implements caddy.StorageConverter.
func (s *KVStorage) CertMagicStorage() (certmagic.Storage, error) {
	return s, nil
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler. Syntax:
//
//	storage enzonix_kv {
//	    endpoint <url>
//	    namespace <namespace>
//	    api_key <key>
//	}
func (s *KVStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		if d.NextArg() {
			// Skip the module name argument if present
		}

		for d.NextBlock(0) {
			switch d.Val() {
			case "endpoint":
				if !d.NextArg() {
					return d.ArgErr()
				}
				s.Endpoint = d.Val()
			case "namespace":
				if !d.NextArg() {
					return d.ArgErr()
				}
				s.Namespace = d.Val()
			case "api_key":
				if !d.NextArg() {
					return d.ArgErr()
				}
				s.APIKey = d.Val()
			default:
				return d.Errf("unrecognized subdirective: %s", d.Val())
			}
		}
	}
	return nil
}

// Store stores a value at the given key.
func (s *KVStorage) Store(ctx context.Context, key string, value []byte) error {
	// Prepare request body - value is stored as plain string
	reqBody := map[string]string{
		"value": string(value),
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create request
	url := fmt.Sprintf("%s/api/write/%s/%s", s.Endpoint, s.Namespace, key)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.APIKey))

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
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.APIKey))

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

	// Parse response - value is a plain string, not base64 encoded
	var result struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return []byte(result.Value), nil
}

// Delete deletes the value at the given key.
func (s *KVStorage) Delete(ctx context.Context, key string) error {
	// Create request
	url := fmt.Sprintf("%s/api/write/%s/%s", s.Endpoint, s.Namespace, key)
	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.APIKey))

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
func (s *KVStorage) listKeys(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	// Get all keys in the namespace
	url := fmt.Sprintf("%s/api/read/%s", s.Endpoint, s.Namespace)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.APIKey))

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
	return s.listKeys(ctx, prefix, recursive)
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
	_ certmagic.Storage      = (*KVStorage)(nil)
	_ caddy.StorageConverter = (*KVStorage)(nil)
	_ caddy.Provisioner      = (*KVStorage)(nil)
	_ caddy.Validator        = (*KVStorage)(nil)
	_ caddyfile.Unmarshaler  = (*KVStorage)(nil)
)
