package kvstorage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

// parseErrorResponse attempts to parse an error response from the API.
// According to API.md, error responses follow the format: {"error": "error message"}
func (s *KVStorage) parseErrorResponse(body []byte) string {
	var errResp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error != "" {
		return errResp.Error
	}
	// Fall back to raw body if not JSON or no error field
	return string(body)
}

// Store stores a value at the given key.
func (s *KVStorage) Store(ctx context.Context, key string, value []byte) error {
	// Base64 encode the value to preserve binary data integrity
	// This prevents JSON encoding from corrupting binary data by escaping special characters
	encodedValue := base64.StdEncoding.EncodeToString(value)

	// Prepare request body - value is stored as base64-encoded string
	reqBody := map[string]string{
		"value": encodedValue,
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create request - URL encode namespace and key to handle special characters
	urlStr := fmt.Sprintf("%s/api/write/%s/%s", s.Endpoint, url.PathEscape(s.Namespace), url.PathEscape(key))
	req, err := http.NewRequestWithContext(ctx, "POST", urlStr, bytes.NewBuffer(jsonBody))
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

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		errorMsg := s.parseErrorResponse(body)
		return fmt.Errorf("storage request failed with status %d: %s", resp.StatusCode, errorMsg)
	}

	// Parse success response to validate (optional, but helps with debugging)
	var result struct {
		Status string `json:"status"`
		Key    string `json:"key"`
	}
	if err := json.Unmarshal(body, &result); err == nil {
		if result.Status != "ok" {
			s.logger.Warn("unexpected response status", zap.String("status", result.Status), zap.String("key", key))
		}
	}

	return nil
}

// Load retrieves a value for the given key.
func (s *KVStorage) Load(ctx context.Context, key string) ([]byte, error) {
	// Create request - URL encode namespace and key to handle special characters
	urlStr := fmt.Sprintf("%s/api/read/%s/%s", s.Endpoint, url.PathEscape(s.Namespace), url.PathEscape(key))
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
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
		errorMsg := s.parseErrorResponse(body)
		return nil, fmt.Errorf("load request failed with status %d: %s", resp.StatusCode, errorMsg)
	}

	// Parse response - value is base64-encoded string
	var result struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Base64 decode the value to restore the original binary data
	// Try base64 decoding first (for new values), fall back to plain text (for backward compatibility)
	decodedValue, err := base64.StdEncoding.DecodeString(result.Value)
	if err != nil {
		// If base64 decoding fails, treat as plain text (backward compatibility with old values)
		// This allows reading values that were stored before the base64 encoding fix
		return []byte(result.Value), nil
	}

	return decodedValue, nil
}

// Delete deletes the value at the given key.
func (s *KVStorage) Delete(ctx context.Context, key string) error {
	// Create request - URL encode namespace and key to handle special characters
	urlStr := fmt.Sprintf("%s/api/write/%s/%s", s.Endpoint, url.PathEscape(s.Namespace), url.PathEscape(key))
	req, err := http.NewRequestWithContext(ctx, "DELETE", urlStr, nil)
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
		errorMsg := s.parseErrorResponse(body)
		return fmt.Errorf("delete request failed with status %d: %s", resp.StatusCode, errorMsg)
	}

	return nil
}

// listKeys returns all keys that have the given prefix.
func (s *KVStorage) listKeys(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	// Get all keys in the namespace - URL encode namespace to handle special characters
	urlStr := fmt.Sprintf("%s/api/read/%s", s.Endpoint, url.PathEscape(s.Namespace))
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
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
		errorMsg := s.parseErrorResponse(body)
		return nil, fmt.Errorf("list request failed with status %d: %s", resp.StatusCode, errorMsg)
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
	lockKey := key + ".lock"
	lockValue := fmt.Sprintf("%d", time.Now().UnixNano())

	// Check if lock already exists
	existingLock, err := s.Load(ctx, lockKey)
	if err == nil {
		// Lock exists - check if it's stale (older than 5 minutes)
		var lockTime int64
		if _, parseErr := fmt.Sscanf(string(existingLock), "%d", &lockTime); parseErr == nil {
			lockAge := time.Since(time.Unix(0, lockTime))
			if lockAge < 5*time.Minute {
				// Lock is still valid, cannot acquire
				return fmt.Errorf("failed to acquire lock: lock already exists")
			}
			// Lock is stale, we can overwrite it
			s.logger.Warn("overwriting stale lock", zap.String("key", lockKey), zap.Duration("age", lockAge))
		} else {
			// Can't parse lock value, assume it's valid
			return fmt.Errorf("failed to acquire lock: lock already exists")
		}
	} else if err != os.ErrNotExist {
		// Some other error occurred
		return fmt.Errorf("failed to check lock existence: %w", err)
	}
	// Lock doesn't exist or is stale, try to create it

	// Try to create the lock key
	err = s.Store(ctx, lockKey, []byte(lockValue))
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Verify the lock was created with our value (defense against race conditions)
	verifyLock, err := s.Load(ctx, lockKey)
	if err != nil {
		return fmt.Errorf("failed to verify lock: %w", err)
	}
	if string(verifyLock) != lockValue {
		// Someone else created the lock between our check and create
		return fmt.Errorf("failed to acquire lock: lock was acquired by another process")
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
