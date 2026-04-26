package xk6_kafka_rest

import (
	"fmt"
	"strings"
)

// validateConfig checks required fields are present and normalises the config.
func validateConfig(cfg *ClientConfig) error {
	var missing []string

	if strings.TrimSpace(cfg.BaseURL) == "" {
		missing = append(missing, "baseUrl")
	} else if strings.Contains(cfg.BaseURL, "/v3/clusters/") || strings.Contains(cfg.BaseURL, "/topics/") {
		return fmt.Errorf(
			"KafkaRestClient: baseUrl should be the REST Proxy root URL only " +
				"(e.g. https://proxy.example.com:443), not a full endpoint path — " +
				"the extension appends the path automatically",
		)
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

	// v3 requires a cluster ID
	if cfg.APIVersion == "v3" && strings.TrimSpace(cfg.ClusterID) == "" {
		missing = append(missing, "clusterId (required for apiVersion: 'v3')")
	}

	if cfg.APIVersion != "" && cfg.APIVersion != "v2" && cfg.APIVersion != "v3" {
		return fmt.Errorf("KafkaRestClient: apiVersion must be 'v2' or 'v3', got %q", cfg.APIVersion)
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"KafkaRestClient: missing required config field(s): %s",
			strings.Join(missing, ", "),
		)
	}

	// Strip trailing slash so endpoint concatenation never produces double slashes.
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")

	return nil
}
