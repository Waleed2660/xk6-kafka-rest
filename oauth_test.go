package xk6_kafka_rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// mockIDPHandler returns an http.HandlerFunc that responds based on the
// client_id sent in the request body, making it easy to simulate different
// IDP failure modes.
func mockIDPHandler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}

		clientID := r.FormValue("client_id")

		switch clientID {
		case "valid-client":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": "test-token-abc123",
				"expires_in":   3600,
				"token_type":   "Bearer",
			})

		case "bad-credentials":
			// Simulate wrong client secret — IDP returns 401
			http.Error(w, `{"error":"invalid_client","error_description":"Bad credentials"}`, http.StatusUnauthorized)

		case "missing-scope":
			// Simulate invalid scope — IDP returns 400
			http.Error(w, `{"error":"invalid_scope","error_description":"Requested scope not allowed"}`, http.StatusBadRequest)

		case "server-error":
			// Simulate transient IDP outage — should trigger retry
			http.Error(w, `{"error":"server_error"}`, http.StatusInternalServerError)

		case "empty-token":
			// IDP returns 200 but with an empty access_token
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": "",
				"expires_in":   3600,
			})

		default:
			http.Error(w, "unknown client", http.StatusUnauthorized)
		}
	}
}

// newTestTokenManager creates a TokenManager pointing at the given test server URL.
func newTestTokenManager(tokenURL, clientID string) *TokenManager {
	return &TokenManager{
		clientID:     clientID,
		clientSecret: "any-secret",
		tokenURL:     tokenURL,
		scope:        "kafka",
		httpClient:   &http.Client{Timeout: 5 * time.Second},
		retry: retryPolicy{
			maxAttempts: 1, // disable retry by default in tests — test retry separately
			baseDelay:   0,
			maxDelay:    0,
		},
	}
}

// ── Happy path ─────────────────────────────────────────────────────────────

func TestTokenManager_ValidCredentials(t *testing.T) {
	srv := httptest.NewServer(mockIDPHandler(t))
	defer srv.Close()

	tm := newTestTokenManager(srv.URL, "valid-client")
	token, err := tm.Token(context.Background())

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if token != "test-token-abc123" {
		t.Errorf("expected token 'test-token-abc123', got %q", token)
	}
}

func TestTokenManager_CachesToken(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "cached-token",
			"expires_in":   3600,
		})
	}))
	defer srv.Close()

	tm := newTestTokenManager(srv.URL, "valid-client")

	// First call should hit the IDP
	if _, err := tm.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Second call should use the cache
	if _, err := tm.Token(context.Background()); err != nil {
		t.Fatal(err)
	}

	if calls != 1 {
		t.Errorf("expected 1 IDP call (token cached), got %d", calls)
	}
}

func TestTokenManager_RefreshesExpiredToken(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "fresh-token",
			"expires_in":   3600,
		})
	}))
	defer srv.Close()

	tm := newTestTokenManager(srv.URL, "valid-client")
	// Pre-seed an already-expired token
	tm.cached = &tokenResponse{
		accessToken: "old-token",
		expiry:      time.Now().Add(-1 * time.Second), // expired
	}

	token, err := tm.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if token != "fresh-token" {
		t.Errorf("expected refreshed token, got %q", token)
	}
	if calls != 1 {
		t.Errorf("expected 1 IDP call for refresh, got %d", calls)
	}
}

// ── Credential / input failures ────────────────────────────────────────────

func TestTokenManager_BadCredentials(t *testing.T) {
	srv := httptest.NewServer(mockIDPHandler(t))
	defer srv.Close()

	tm := newTestTokenManager(srv.URL, "bad-credentials")
	_, err := tm.Token(context.Background())

	if err == nil {
		t.Fatal("expected error for bad credentials, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("expected 401 in error, got: %v", err)
	}
}

func TestTokenManager_InvalidScope(t *testing.T) {
	srv := httptest.NewServer(mockIDPHandler(t))
	defer srv.Close()

	tm := newTestTokenManager(srv.URL, "missing-scope")
	_, err := tm.Token(context.Background())

	if err == nil {
		t.Fatal("expected error for invalid scope, got nil")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("expected 400 in error, got: %v", err)
	}
}

func TestTokenManager_EmptyTokenResponse(t *testing.T) {
	srv := httptest.NewServer(mockIDPHandler(t))
	defer srv.Close()

	tm := newTestTokenManager(srv.URL, "empty-token")
	_, err := tm.Token(context.Background())

	if err == nil {
		t.Fatal("expected error for empty token, got nil")
	}
	if !strings.Contains(err.Error(), "empty access_token") {
		t.Errorf("expected 'empty access_token' in error, got: %v", err)
	}
}

func TestTokenManager_MissingTokenURL(t *testing.T) {
	tm := newTestTokenManager("http://127.0.0.1:0", "valid-client") // nothing listening
	_, err := tm.Token(context.Background())

	if err == nil {
		t.Fatal("expected error for unreachable token URL, got nil")
	}
}

// ── Retry behaviour ────────────────────────────────────────────────────────

func TestTokenManager_RetriesOn5xx(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			// Fail the first two attempts with 503
			http.Error(w, `{"error":"server_error"}`, http.StatusServiceUnavailable)
			return
		}
		// Succeed on the third attempt
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "retry-token",
			"expires_in":   3600,
		})
	}))
	defer srv.Close()

	tm := newTestTokenManager(srv.URL, "valid-client")
	tm.retry = retryPolicy{maxAttempts: 4, baseDelay: 0, maxDelay: 0} // fast retry

	token, err := tm.Token(context.Background())
	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if token != "retry-token" {
		t.Errorf("expected 'retry-token', got %q", token)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestTokenManager_DoesNotRetryOn4xx(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()

	tm := newTestTokenManager(srv.URL, "any")
	tm.retry = retryPolicy{maxAttempts: 4, baseDelay: 0, maxDelay: 0}

	_, err := tm.Token(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt (no retry on 4xx), got %d", attempts)
	}
}

func TestTokenManager_ExhaustsRetries(t *testing.T) {
	srv := httptest.NewServer(mockIDPHandler(t))
	defer srv.Close()

	tm := newTestTokenManager(srv.URL, "server-error")
	tm.retry = retryPolicy{maxAttempts: 3, baseDelay: 0, maxDelay: 0}

	_, err := tm.Token(context.Background())
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if !strings.Contains(err.Error(), "all 3 attempts failed") {
		t.Errorf("expected 'all 3 attempts failed' in error, got: %v", err)
	}
}

func TestTokenManager_ContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a slow IDP — respond only after context is done or 1s, whichever first.
		select {
		case <-r.Context().Done():
		case <-time.After(1 * time.Second):
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	tm := newTestTokenManager(srv.URL, "valid-client")
	_, err := tm.Token(ctx)

	if err == nil {
		t.Fatal("expected error on context cancellation, got nil")
	}
}
