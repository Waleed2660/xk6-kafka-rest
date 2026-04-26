package xk6_kafka_rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/modules"
)

const defaultMaxBatchSize = 500

// ClientConfig holds all options passed from the JS constructor.
type ClientConfig struct {
	BaseURL      string `js:"baseUrl"`
	ClientID     string `js:"clientId"`
	ClientSecret string `js:"clientSecret"`
	TokenURL     string `js:"tokenUrl"`
	Scope        string `js:"scope"`
	MaxBatchSize int    `js:"maxBatchSize"`
}

// Message represents a single Kafka record sent to the REST Proxy.
type Message struct {
	Key   interface{} `json:"key,omitempty"`
	Value interface{} `json:"value"`
}

// messageWire is the on-the-wire representation sent to the REST Proxy v2 JSON API.
type messageWire struct {
	Key   interface{} `json:"key,omitempty"`
	Value interface{} `json:"value"`
}

// toWire converts a Message to its REST Proxy v2 JSON wire format.
func toWire(m Message) messageWire {
	return messageWire(m)
}

// ProduceResponse is returned to the JS script after a successful publish.
type ProduceResponse struct {
	KeySchemaID   int              `json:"key_schema_id,omitempty"`
	ValueSchemaID int              `json:"value_schema_id,omitempty"`
	Offsets       []OffsetMetadata `json:"offsets"`
}

// OffsetMetadata describes where a single record landed in Kafka.
type OffsetMetadata struct {
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	ErrorCode *int   `json:"error_code,omitempty"`
	Error     string `json:"error,omitempty"`
}

// KafkaRestClient is the object exposed to k6 JS scripts.
type KafkaRestClient struct {
	vu           modules.VU
	config       ClientConfig
	tokenManager *TokenManager
	httpClient   *http.Client
	metrics      *kafkaMetrics
}

// newKafkaRestClient is the JS constructor: new KafkaRestClient(config).
// sobek.ConstructorCall is required for k6 to allow calling it with `new`.
func (m *KafkaRestModule) newKafkaRestClient(call sobek.ConstructorCall) *sobek.Object {
	rt := m.vu.Runtime()

	var config ClientConfig
	if len(call.Arguments) > 0 {
		if err := rt.ExportTo(call.Arguments[0], &config); err != nil {
			panic(rt.NewTypeError("KafkaRestClient: invalid config: " + err.Error()))
		}
	}

	if err := validateConfig(config); err != nil {
		panic(rt.NewTypeError(err.Error()))
	}

	client := &KafkaRestClient{
		vu:           m.vu,
		config:       config,
		tokenManager: NewTokenManager(config),
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		metrics:      m.metrics,
	}

	return rt.ToValue(client).ToObject(rt)
}

// Produce publishes messages to a Kafka topic, auto-chunking into batches
// of maxBatchSize (default 500, ceiling 1000) to stay within REST Proxy limits.
func (c *KafkaRestClient) Produce(topic string, messages []Message) (*ProduceResponse, error) {
	chunkSize := c.config.MaxBatchSize
	if chunkSize <= 0 {
		chunkSize = defaultMaxBatchSize
	}
	const hardCeiling = 1000
	if chunkSize > hardCeiling {
		chunkSize = hardCeiling
	}

	var allOffsets []OffsetMetadata

	for i := 0; i < len(messages); i += chunkSize {
		end := i + chunkSize
		if end > len(messages) {
			end = len(messages)
		}
		chunk := messages[i:end]

		ctx := context.Background()
		start := time.Now()

		result, produceErr := c.doProduce(ctx, topic, chunk)

		// The REST Proxy returns 200 OK even when individual records fail —
		// each offset entry carries its own error_code.
		successCount, failedCount := 0, 0
		var recordErrors []string
		if result != nil {
			for j, off := range result.Offsets {
				if off.ErrorCode != nil && *off.ErrorCode != 0 {
					failedCount++
					recordErrors = append(recordErrors,
						fmt.Sprintf("record[%d] error_code=%d: %s", i+j, *off.ErrorCode, off.Error))
				} else {
					successCount++
				}
			}
			allOffsets = append(allOffsets, result.Offsets...)
		} else if produceErr != nil {
			failedCount = len(chunk)
		}

		c.pushSamples(ctx, topic, successCount, failedCount, time.Since(start))

		if produceErr != nil {
			return &ProduceResponse{Offsets: allOffsets}, produceErr
		}
		if len(recordErrors) > 0 {
			preview := recordErrors
			if len(preview) > 3 {
				preview = append(preview[:3], fmt.Sprintf("…and %d more", len(recordErrors)-3))
			}
			return &ProduceResponse{Offsets: allOffsets},
				fmt.Errorf("kafka-rest produce: %d/%d records failed — %s",
					failedCount, len(chunk), strings.Join(preview, "; "))
		}
	}

	return &ProduceResponse{Offsets: allOffsets}, nil
}

func (c *KafkaRestClient) doProduce(ctx context.Context, topic string, messages []Message) (*ProduceResponse, error) {
	token, err := c.tokenManager.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka-rest produce: %w", err)
	}

	wire := make([]messageWire, len(messages))
	for i, m := range messages {
		wire[i] = toWire(m)
	}
	bodyBytes, err := json.Marshal(map[string]interface{}{"records": wire})
	if err != nil {
		return nil, fmt.Errorf("kafka-rest produce: marshal: %w", err)
	}

	endpoint := fmt.Sprintf("%s/topics/%s", c.config.BaseURL, topic)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("kafka-rest produce: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.kafka.json.v2+json")
	req.Header.Set("Accept", "application/vnd.kafka.v2+json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kafka-rest produce: http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errBody struct {
			ErrorCode int    `json:"error_code"`
			Message   string `json:"message"`
		}
		_ = jsonDecode(resp.Body, &errBody)
		if errBody.Message != "" {
			return nil, fmt.Errorf("kafka-rest produce: REST proxy returned %s — %s (error_code=%d)",
				resp.Status, errBody.Message, errBody.ErrorCode)
		}
		return nil, fmt.Errorf("kafka-rest produce: REST proxy returned %s", resp.Status)
	}

	var result ProduceResponse
	if err := jsonDecode(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("kafka-rest produce: decode response: %w", err)
	}
	return &result, nil
}

// Close is a no-op reserved for future connection cleanup.
func (c *KafkaRestClient) Close() {}
