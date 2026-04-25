package xk6_kafka_rest

import (
	"context"
	"time"

	"go.k6.io/k6/metrics"
)

// kafkaMetrics holds the three custom k6 metrics for this extension.
type kafkaMetrics struct {
	MessagesSent    *metrics.Metric
	PublishDuration *metrics.Metric
	PublishErrors   *metrics.Metric
}

// registerMetrics registers all custom metrics against the k6 registry.
// Safe to call once per VU — k6's registry deduplicates by name+type.
func registerMetrics(registry *metrics.Registry) *kafkaMetrics {
	return &kafkaMetrics{
		MessagesSent:    registry.MustNewMetric("kafka_rest_messages_sent", metrics.Counter),
		PublishDuration: registry.MustNewMetric("kafka_rest_publish_duration", metrics.Trend, metrics.Time),
		PublishErrors:   registry.MustNewMetric("kafka_rest_publish_errors", metrics.Counter),
	}
}

// pushSamples records metric samples after a produce attempt.
// successCount — records confirmed by Kafka (error_code == 0).
// failedCount  — records rejected by Kafka OR the whole call failed.
func (c *KafkaRestClient) pushSamples(ctx context.Context, topic string, successCount, failedCount int, duration time.Duration) {
	state := c.vu.State()
	if state == nil {
		return
	}

	now := time.Now()
	tags := state.Tags.GetCurrentValues().Tags.With("topic", topic)

	samples := []metrics.Sample{
		{
			TimeSeries: metrics.TimeSeries{Metric: c.metrics.PublishDuration, Tags: tags},
			Value:      float64(duration) / float64(time.Millisecond),
			Time:       now,
		},
	}
	if successCount > 0 {
		samples = append(samples, metrics.Sample{
			TimeSeries: metrics.TimeSeries{Metric: c.metrics.MessagesSent, Tags: tags},
			Value:      float64(successCount),
			Time:       now,
		})
	}
	if failedCount > 0 {
		samples = append(samples, metrics.Sample{
			TimeSeries: metrics.TimeSeries{Metric: c.metrics.PublishErrors, Tags: tags},
			Value:      float64(failedCount),
			Time:       now,
		})
	}

	metrics.PushIfNotDone(ctx, state.Samples, metrics.ConnectedSamples{
		Samples: samples,
		Tags:    tags,
		Time:    now,
	})
}
