package usecases

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"inbox/internal/domain"
	"inbox/internal/metrics"
)

type InboxRepository interface {
	GetByTransferID(ctx context.Context, transferID string) (*domain.InboxRecord, error)
	Save(ctx context.Context, r *domain.InboxRecord) error
}

type ProcessedPaymentRepository interface {
	Create(ctx context.Context, p *domain.ProcessedPayment) error
}

type DeadLetterQueue interface {
	Put(ctx context.Context, msg domain.DeadLetterMessage) error
}

type PaymentUseCase struct {
	inbox     InboxRepository
	processed ProcessedPaymentRepository
	dlq       DeadLetterQueue
	metrics   *metrics.Metrics
	topic     string
}

// NewPaymentUseCase. topic нужен только чтобы метки duplicates_total/kafka
// были осмысленными на дашборде — он не используется в бизнес-логике.
func NewPaymentUseCase(
	inbox InboxRepository,
	processed ProcessedPaymentRepository,
	dlq DeadLetterQueue,
	m *metrics.Metrics,
	topic string,
) *PaymentUseCase {
	return &PaymentUseCase{
		inbox:     inbox,
		processed: processed,
		dlq:       dlq,
		metrics:   m,
		topic:     topic,
	}
}

func (uc *PaymentUseCase) ProcessPayment(
	ctx context.Context,
	msg domain.TransferMessage,
) error {
	start := time.Now()
	// outcome обновляется по ходу обработки; defer фиксирует итоговый исход
	// в гистограмму и счётчик. При ранних return достаточно установить
	// outcome до return — это безопаснее, чем строить отдельные defer'ы.
	outcome := metrics.OutcomeProcessed
	defer func() {
		uc.metrics.ProcessingDuration.WithLabelValues(string(outcome)).Observe(time.Since(start).Seconds())
		uc.metrics.MessagesProcessed.WithLabelValues(string(outcome)).Inc()
	}()

	existing, err := uc.inbox.GetByTransferID(ctx, msg.TransferID)
	if err != nil {
		outcome = metrics.OutcomeProcessingError
		return err
	}

	if existing == nil {
		err = uc.inbox.Save(ctx, &domain.InboxRecord{
			TransferID: msg.TransferID,
			Status:     domain.StatusReceived,
			Payload:    marshal(msg),
		})
		if err != nil {
			outcome = metrics.OutcomeProcessingError
			return err
		}
	} else if existing.Status == domain.StatusProcessed {
		// Дедупликация: то же transfer_id уже обработано. Замеряем явно —
		// это ключевой сигнал, что at-least-once реально работает и в нём
		// есть смысл (повторные сообщения действительно приходят и
		// отбрасываются inbox-ом).
		outcome = metrics.OutcomeDuplicate
		uc.metrics.DuplicatesDetected.WithLabelValues(uc.topic).Inc()
		return nil
	}

	if vErr := validate(msg); vErr != nil {
		outcome = metrics.OutcomeValidationError
		// failingField возвращает поле из текста ошибки validate; набор
		// конечный, поэтому кардинальность метки контролируется.
		uc.metrics.ValidationErrors.WithLabelValues(failingField(vErr)).Inc()

		_ = uc.inbox.Save(ctx, &domain.InboxRecord{
			TransferID: msg.TransferID,
			Status:     domain.StatusFailed,
			Payload:    marshal(msg),
		})

		_ = uc.dlq.Put(ctx, domain.DeadLetterMessage{
			TransferID: msg.TransferID,
			Payload:    string(marshal(msg)),
			Error:      vErr.Error(),
			ErrorType:  string(metrics.ErrorTypeValidation),
		})

		return nil
	}

	err = uc.processed.Create(ctx, &domain.ProcessedPayment{
		TransferID:  msg.TransferID,
		Amount:      msg.Amount,
		FromAccount: msg.FromAccount,
		ToAccount:   msg.ToAccount,
	})
	if err != nil {
		outcome = metrics.OutcomeProcessingError

		_ = uc.inbox.Save(ctx, &domain.InboxRecord{
			TransferID: msg.TransferID,
			Status:     domain.StatusFailed,
			Payload:    marshal(msg),
		})

		_ = uc.dlq.Put(ctx, domain.DeadLetterMessage{
			TransferID: msg.TransferID,
			Payload:    string(marshal(msg)),
			Error:      err.Error(),
			ErrorType:  string(metrics.ErrorTypeProcessing),
		})

		return err
	}

	if err := uc.inbox.Save(ctx, &domain.InboxRecord{
		TransferID: msg.TransferID,
		Status:     domain.StatusProcessed,
		Payload:    marshal(msg),
	}); err != nil {
		outcome = metrics.OutcomeProcessingError
		return err
	}

	// E2E считаем только для свежеобработанных сообщений: дубликаты
	// и валидационные ошибки делали бы метрику нерепрезентативной.
	// EventTime приходит из outbox.MoneyTransferEvent.EventTime;
	// если по какой-то причине пуст — пропускаем наблюдение.
	if !msg.EventTime.IsZero() {
		uc.metrics.DeliveryE2ELatency.Observe(time.Since(msg.EventTime).Seconds())
	}

	return nil
}

func marshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func validate(msg domain.TransferMessage) error {
	if msg.TransferID == "" {
		return errFieldMissing("transfer_id")
	}
	if msg.Amount < 0 {
		return errFieldInvalid("amount")
	}
	if msg.FromAccount == "" {
		return errFieldMissing("from_account")
	}
	if msg.ToAccount == "" {
		return errFieldMissing("to_account")
	}
	return nil
}

// fieldError позволяет в одной строке достать имя упавшего поля для метки.
// Альтернатива — парсить ошибку строкой; явный тип проще и быстрее.
type fieldError struct {
	field string
	msg   string
}

func (e *fieldError) Error() string { return e.msg + " " + e.field }

func errFieldMissing(field string) error { return &fieldError{field: field, msg: "missing"} }
func errFieldInvalid(field string) error { return &fieldError{field: field, msg: "invalid"} }

func failingField(err error) string {
	var fe *fieldError
	if errors.As(err, &fe) {
		return fe.field
	}
	return "unknown"
}
