package dlq

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"

	"inbox/internal/domain"
)

type DLQProducer struct {
	writer *kafka.Writer
	topic  string
}

func NewDLQProducer(brokers []string, topic string) *DLQProducer {
	return &DLQProducer{
		topic: topic,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *DLQProducer) Put(ctx context.Context, msg domain.DeadLetterMessage) error {

	msg.Timestamp = time.Now().Unix()

	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: p.topic,
		Value: b,
	})
}
