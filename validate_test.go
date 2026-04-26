package xk6_kafka_rest

import (
	"strings"
	"testing"
)

func TestValidateConfig_AllFieldsPresent(t *testing.T) {
	cfg := ClientConfig{
		BaseURL:      "http://localhost:8082",
		TokenURL:     "http://localhost:8080/token",
		ClientID:     "my-client",
		ClientSecret: "my-secret",
	}
	if err := validateConfig(&cfg); err != nil {
		t.Errorf("expected no error for valid config, got: %v", err)
	}
}

func TestValidateConfig_AllFieldsMissing(t *testing.T) {
	cfg := ClientConfig{}
	err := validateConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for empty config, got nil")
	}
	for _, field := range []string{"baseUrl", "tokenUrl", "clientId", "clientSecret"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("expected error to mention %q, got: %v", field, err)
		}
	}
}

func TestValidateConfig_MissingBaseURL(t *testing.T) {
	cfg := ClientConfig{TokenURL: "http://idp/token", ClientID: "id", ClientSecret: "secret"}
	err := validateConfig(&cfg)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "baseUrl") {
		t.Errorf("expected 'baseUrl' in error, got: %v", err)
	}
}

func TestValidateConfig_MissingTokenURL(t *testing.T) {
	cfg := ClientConfig{BaseURL: "http://proxy", ClientID: "id", ClientSecret: "secret"}
	err := validateConfig(&cfg)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "tokenUrl") {
		t.Errorf("expected 'tokenUrl' in error, got: %v", err)
	}
}

func TestValidateConfig_MissingCredentials(t *testing.T) {
	cfg := ClientConfig{BaseURL: "http://proxy", TokenURL: "http://idp/token"}
	err := validateConfig(&cfg)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "clientId") {
		t.Errorf("expected 'clientId' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "clientSecret") {
		t.Errorf("expected 'clientSecret' in error, got: %v", err)
	}
}

func TestValidateConfig_WhitespaceFieldsTreatedAsEmpty(t *testing.T) {
	cfg := ClientConfig{
		BaseURL:      "   ",
		TokenURL:     "\t",
		ClientID:     "id",
		ClientSecret: "secret",
	}
	err := validateConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for whitespace-only fields, got nil")
	}
	if !strings.Contains(err.Error(), "baseUrl") {
		t.Errorf("expected 'baseUrl' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "tokenUrl") {
		t.Errorf("expected 'tokenUrl' in error, got: %v", err)
	}
}

func TestValidateConfig_TrailingSlashStripped(t *testing.T) {
	cfg := ClientConfig{
		BaseURL:      "http://localhost:8082/",
		TokenURL:     "http://localhost:8080/token",
		ClientID:     "id",
		ClientSecret: "secret",
	}
	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.BaseURL != "http://localhost:8082" {
		t.Errorf("expected trailing slash stripped, got %q", cfg.BaseURL)
	}
}

func TestValidateConfig_RejectsV3PathInBaseURL(t *testing.T) {
	cfg := ClientConfig{
		BaseURL:      "https://proxy.example.com/kafka/v3/clusters/lkc-abc/topics/%s/records",
		TokenURL:     "http://idp/token",
		ClientID:     "id",
		ClientSecret: "secret",
	}
	err := validateConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for v3 path in baseUrl, got nil")
	}
	if !strings.Contains(err.Error(), "REST Proxy root URL") {
		t.Errorf("expected helpful message, got: %v", err)
	}
}

func TestValidateConfig_RejectsTopicsPathInBaseURL(t *testing.T) {
	cfg := ClientConfig{
		BaseURL:      "https://proxy.example.com/topics/my-topic",
		TokenURL:     "http://idp/token",
		ClientID:     "id",
		ClientSecret: "secret",
	}
	err := validateConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for /topics/ path in baseUrl, got nil")
	}
}
