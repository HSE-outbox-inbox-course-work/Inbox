package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/segmentio/kafka-go"

	"inbox/internal/domain"
	"inbox/internal/metrics"
)

type PaymentProcessor interface {
	ProcessPayment(ctx context.Context, msg domain.TransferMessage) error
}

type Listener struct {
	reader  *kafka.Reader
	uc      PaymentProcessor
	metrics *metrics.Metrics
	topic   string
}

func NewListener(brokers []string, topic, group string, uc PaymentProcessor, m *metrics.Metrics) *Listener {
	return &Listener{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     group,
			StartOffset: kafka.FirstOffset,
		}),
		uc:      uc,
		metrics: m,
		topic:   topic,
	}
}

type Envelope struct {
	Payload string `json:"payload"`
}

// outcome для kafka_messages_read_total — короткий конечный набор:
// успех или конкретная стадия, на которой сломались.
const (
	readOutcomeOK            = "ok"
	readOutcomeReadError     = "read_error"
	readOutcomeBadEnvelope   = "bad_envelope"
	readOutcomeBadPayload    = "bad_payload"
)

func (l *Listener) Listen(ctx context.Context) {
	defer l.reader.Close()

	for {
		msg, err := l.reader.ReadMessage(ctx)
		if err != nil {
			// Контекст закрыт штатно при shutdown — не считаем это ошибкой
			// чтения, иначе на каждом выходе будет ложный пик метрики.
			if errors.Is(err, context.Canceled) {
				return
			}
			l.metrics.KafkaMessagesRead.WithLabelValues(l.topic, readOutcomeReadError).Inc()
			log.Println("read error:", err)
			continue
		}

		var env Envelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			l.metrics.KafkaMessagesRead.WithLabelValues(l.topic, readOutcomeBadEnvelope).Inc()
			log.Println("bad envelope:", err)
			continue
		}

		var transfer domain.TransferMessage
		if err := json.Unmarshal([]byte(env.Payload), &transfer); err != nil {
			l.metrics.KafkaMessagesRead.WithLabelValues(l.topic, readOutcomeBadPayload).Inc()
			log.Println("bad payload:", err)
			continue
		}

		l.metrics.KafkaMessagesRead.WithLabelValues(l.topic, readOutcomeOK).Inc()

		if err := l.uc.ProcessPayment(ctx, transfer); err != nil {
			log.Println("usecase error:", err)
		}
	}
}
