package dlq

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"

	"inbox/internal/domain"
	"inbox/internal/metrics"
)

type DLQProducer struct {
	writer  *kafka.Writer
	topic   string
	metrics *metrics.Metrics
}

func NewDLQProducer(brokers []string, topic string, m *metrics.Metrics) *DLQProducer {
	return &DLQProducer{
		topic: topic,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Balancer: &kafka.LeastBytes{},
		},
		metrics: m,
	}
}

// outcome для счётчика DLQ — публикация либо прошла, либо упала на одной
// из двух стадий. Эти три значения покрывают всё, метки не разрастаются.
const (
	dlqOutcomeOK         = "ok"
	dlqOutcomeMarshalErr = "marshal_error"
	dlqOutcomeWriteErr   = "write_error"
)

func (p *DLQProducer) Put(ctx context.Context, msg domain.DeadLetterMessage) error {
	start := time.Now()

	msg.Timestamp = time.Now().Unix()

	b, err := json.Marshal(msg)
	if err != nil {
		p.metrics.DLQMessagesProduced.WithLabelValues(msg.ErrorType, dlqOutcomeMarshalErr).Inc()
		p.metrics.DLQProduceDuration.WithLabelValues(dlqOutcomeMarshalErr).Observe(time.Since(start).Seconds())
		return err
	}

	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Topic: p.topic,
		Value: b,
	}); err != nil {
		p.metrics.DLQMessagesProduced.WithLabelValues(msg.ErrorType, dlqOutcomeWriteErr).Inc()
		p.metrics.DLQProduceDuration.WithLabelValues(dlqOutcomeWriteErr).Observe(time.Since(start).Seconds())
		return err
	}

	p.metrics.DLQMessagesProduced.WithLabelValues(msg.ErrorType, dlqOutcomeOK).Inc()
	p.metrics.DLQProduceDuration.WithLabelValues(dlqOutcomeOK).Observe(time.Since(start).Seconds())
	return nil
}
