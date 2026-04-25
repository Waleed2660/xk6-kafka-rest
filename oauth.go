package xk6_kafka_rest

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// retryPolicy controls how token fetches are retried on transient failures.
type retryPolicy struct {
	maxAttempts int
	baseDelay   time.Duration // delay before attempt 2; doubles each attempt
	maxDelay    time.Duration
}

var defaultRetryPolicy = retryPolicy{
	maxAttempts: 4,
	baseDelay:   500 * time.Millisecond,
	maxDelay:    10 * time.Second,
}

// tokenResponse holds the parsed OAuth token endpoint response.
type tokenResponse struct {
	accessToken string
	expiry      time.Time
}

// TokenManager fetches and caches an OAuth client-credentials token.
// It is safe for concurrent use across goroutines (VUs).
type TokenManager struct {
	mu           sync.Mutex
	clientID     string
	clientSecret string
	tokenURL     string
	scope        string
	cached       *tokenResponse
	httpClient   *http.Client
	retry        retryPolicy
}

// NewTokenManager creates a TokenManager from a ClientConfig.
func NewTokenManager(cfg ClientConfig) *TokenManager {
	return &TokenManager{
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		tokenURL:     cfg.TokenURL,
		scope:        cfg.Scope,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		retry:        defaultRetryPolicy,
	}
}

// Token returns a valid Bearer token, refreshing it when fewer than
// 30 seconds remain before expiry.
func (t *TokenManager) Token(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.cached != nil && time.Until(t.cached.expiry) > 30*time.Second {
		return t.cached.accessToken, nil
	}

	return t.fetchWithRetry(ctx)
}

// fetchWithRetry calls fetchToken up to retry.maxAttempts times, backing off
// exponentially between attempts. Only retries on network errors and HTTP 5xx;
// 4xx responses (bad credentials etc.) are returned immediately.
func (t *TokenManager) fetchWithRetry(ctx context.Context) (string, error) {
	delay := t.retry.baseDelay
	var lastErr error

	for attempt := 1; attempt <= t.retry.maxAttempts; attempt++ {
		token, err := t.fetchToken(ctx)
		if err == nil {
			return token, nil
		}

		lastErr = err

		// Do not retry on permanent errors (4xx) or if context is done.
		if isFatal(err) || ctx.Err() != nil {
			break
		}

		if attempt < t.retry.maxAttempts {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
			if delay > t.retry.maxDelay {
				delay = t.retry.maxDelay
			}
		}
	}

	return "", fmt.Errorf("oauth: all %d attempts failed, last error: %w", t.retry.maxAttempts, lastErr)
}

// isFatal returns true for errors that should not be retried (4xx status codes).
func isFatal(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	// fetchToken embeds the HTTP status in the message for non-200 responses.
	// 4xx = permanent failure (bad credentials, invalid scope, etc.)
	for _, prefix := range []string{" 4"} {
		if strings.Contains(s, prefix) {
			return true
		}
	}
	return false
}

// fetchToken performs a single client-credentials grant attempt.
// Must be called with t.mu held.
func (t *TokenManager) fetchToken(ctx context.Context) (string, error) {
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", t.clientID)
	data.Set("client_secret", t.clientSecret)
	if t.scope != "" {
		data.Set("scope", t.scope)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.tokenURL,
		strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("oauth: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("oauth: token request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("oauth: token endpoint returned %s", resp.Status)
	}

	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := jsonDecode(resp.Body, &body); err != nil {
		return "", fmt.Errorf("oauth: decode response: %w", err)
	}
	if body.AccessToken == "" {
		return "", fmt.Errorf("oauth: empty access_token in response")
	}

	expiry := time.Now().Add(time.Duration(body.ExpiresIn) * time.Second)
	t.cached = &tokenResponse{accessToken: body.AccessToken, expiry: expiry}
	return body.AccessToken, nil
}
