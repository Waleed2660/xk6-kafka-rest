package xk6_kafka_rest

import (
	"fmt"
	"strings"
)

func validateConfig(cfg ClientConfig) error {
	var missing []string

	if strings.TrimSpace(cfg.BaseURL) == "" {
		missing = append(missing, "baseUrl")
	}
	if strings.TrimSpace(cfg.TokenURL) == "" {
		missing = append(missing, "tokenUrl")
	}
	if strings.TrimSpace(cfg.ClientID) == "" {
		missing = append(missing, "clientId")
	}
	if strings.TrimSpace(cfg.ClientSecret) == "" {
		missing = append(missing, "clientSecret")
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"KafkaRestClient: missing required config field(s): %s",
			strings.Join(missing, ", "),
		)
	}
	return nil
}
