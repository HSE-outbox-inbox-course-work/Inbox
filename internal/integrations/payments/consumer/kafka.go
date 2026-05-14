package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"

	"inbox/internal/domain"
)

type PaymentProcessor interface {
	ProcessPayment(ctx context.Context, msg domain.TransferMessage) error
}

type Listener struct {
	reader *kafka.Reader
	uc     PaymentProcessor
}

func NewListener(brokers []string, topic, group string, uc PaymentProcessor) *Listener {
	return &Listener{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     group,
			StartOffset: kafka.FirstOffset,
		}),
		uc: uc,
	}
}

type Envelope struct {
	Payload string `json:"payload"`
}

func (l *Listener) Listen(ctx context.Context) {
	defer l.reader.Close()

	for {
		msg, err := l.reader.ReadMessage(ctx)
		if err != nil {
			log.Println("read error:", err)
			continue
		}

		var env Envelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			log.Println("bad envelope:", err)
			continue
		}

		var transfer domain.TransferMessage
		if err := json.Unmarshal([]byte(env.Payload), &transfer); err != nil {
			log.Println("bad payload:", err)
			continue
		}

		if err := l.uc.ProcessPayment(ctx, transfer); err != nil {
			log.Println("usecase error:", err)
		}
	}
}
