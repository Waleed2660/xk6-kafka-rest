package xk6_kafka_rest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

const defaultV3Concurrency = 20

// v3RecordRequest is the body sent to POST /v3/clusters/{id}/topics/{topic}/records.
type v3RecordRequest struct {
	Key     *v3Data    `json:"key,omitempty"`
	Value   v3Data     `json:"value"`
	Headers []v3Header `json:"headers,omitempty"`
}

type v3Data struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// v3Header name/value pair — value must be base64-encoded per the REST Proxy spec.
type v3Header struct {
	Name  string `json:"name"`
	Value string `json:"value"` // base64-encoded
}

type v3ProduceResponse struct {
	PartitionID int    `json:"partition_id"`
	Offset      int64  `json:"offset"`
	ErrorCode   *int   `json:"error_code,omitempty"`
	Message     string `json:"message,omitempty"`
}

// produceV3 sends all messages concurrently (bounded by maxBatchSize) to the v3 API.
// Each message is a separate HTTP call; results are collected and returned in order.
func (c *KafkaRestClient) produceV3(ctx context.Context, topic string, messages []Message) ([]OffsetMetadata, error) {
	concurrency := c.config.MaxBatchSize
	if concurrency <= 0 {
		concurrency = defaultV3Concurrency
	}
	if concurrency > 100 {
		concurrency = 100
	}

	type result struct {
		offset OffsetMetadata
		err    error
	}

	results := make([]result, len(messages))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i, msg := range messages {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, m Message) {
			defer wg.Done()
			defer func() { <-sem }()
			off, err := c.doProduceV3Record(ctx, topic, m)
			results[idx] = result{offset: off, err: err}
		}(i, msg)
	}
	wg.Wait()

	offsets := make([]OffsetMetadata, 0, len(messages))
	for _, r := range results {
		if r.err != nil {
			return offsets, r.err
		}
		offsets = append(offsets, r.offset)
	}
	return offsets, nil
}

func (c *KafkaRestClient) doProduceV3Record(ctx context.Context, topic string, msg Message) (OffsetMetadata, error) {
	token, err := c.tokenManager.Token(ctx)
	if err != nil {
		return OffsetMetadata{}, fmt.Errorf("kafka-rest v3: %w", err)
	}

	body := v3RecordRequest{
		Value: v3Data{Type: "JSON", Data: msg.Value},
	}
	if msg.Key != nil {
		// String keys use type STRING; objects use JSON.
		if s, ok := msg.Key.(string); ok {
			body.Key = &v3Data{Type: "STRING", Data: s}
		} else {
			body.Key = &v3Data{Type: "JSON", Data: msg.Key}
		}
	}
	for _, h := range msg.Headers {
		body.Headers = append(body.Headers, v3Header{
			Name:  h.Key,
			Value: base64.StdEncoding.EncodeToString([]byte(h.Value)),
		})
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return OffsetMetadata{}, fmt.Errorf("kafka-rest v3: marshal: %w", err)
	}

	endpoint := fmt.Sprintf("%s/v3/clusters/%s/topics/%s/records",
		c.config.BaseURL, c.config.ClusterID, topic)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return OffsetMetadata{}, fmt.Errorf("kafka-rest v3: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return OffsetMetadata{}, fmt.Errorf("kafka-rest v3: http: %w", err)
	}
	defer resp.Body.Close()

	var v3Resp v3ProduceResponse
	_ = jsonDecode(resp.Body, &v3Resp)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if v3Resp.Message != "" {
			return OffsetMetadata{}, fmt.Errorf("kafka-rest v3: REST proxy returned %s — %s", resp.Status, v3Resp.Message)
		}
		return OffsetMetadata{}, fmt.Errorf("kafka-rest v3: REST proxy returned %s", resp.Status)
	}

	return OffsetMetadata{
		Partition: v3Resp.PartitionID,
		Offset:    v3Resp.Offset,
		ErrorCode: v3Resp.ErrorCode,
	}, nil
}
