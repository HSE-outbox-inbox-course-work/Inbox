package usecases

import (
	"context"
	"encoding/json"
	"errors"

	"inbox/internal/domain"
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
}

func NewPaymentUseCase(
	inbox InboxRepository,
	processed ProcessedPaymentRepository,
	dlq DeadLetterQueue,
) *PaymentUseCase {
	return &PaymentUseCase{
		inbox:     inbox,
		processed: processed,
		dlq:       dlq,
	}
}
func (uc *PaymentUseCase) ProcessPayment(
	ctx context.Context,
	msg domain.TransferMessage,
) error {

	// 1. СНАЧАЛА фиксируем факт получения (idempotency entry)
	existing, err := uc.inbox.GetByTransferID(ctx, msg.TransferID)
	if err != nil {
		return err
	}

	if existing == nil {
		err = uc.inbox.Save(ctx, &domain.InboxRecord{
			TransferID: msg.TransferID,
			Status:     domain.StatusReceived,
			Payload:    marshal(msg),
		})
		if err != nil {
			return err
		}
	} else if existing.Status == domain.StatusProcessed {
		// уже обработано — выходим
		return nil
	}

	// 2. валидация (теперь после фикса RECEIVED)
	if err := validate(msg); err != nil {

		_ = uc.inbox.Save(ctx, &domain.InboxRecord{
			TransferID: msg.TransferID,
			Status:     domain.StatusFailed,
			Payload:    marshal(msg),
		})

		_ = uc.dlq.Put(ctx, domain.DeadLetterMessage{
			TransferID: msg.TransferID,
			Payload:    string(marshal(msg)),
			Error:      err.Error(),
			ErrorType:  "VALIDATION_ERROR",
		})

		return nil
	}

	// 3. бизнес обработка
	err = uc.processed.Create(ctx, &domain.ProcessedPayment{
		TransferID:  msg.TransferID,
		Amount:      msg.Amount,
		FromAccount: msg.FromAccount,
		ToAccount:   msg.ToAccount,
	})
	if err != nil {

		_ = uc.inbox.Save(ctx, &domain.InboxRecord{
			TransferID: msg.TransferID,
			Status:     domain.StatusFailed,
			Payload:    marshal(msg),
		})

		_ = uc.dlq.Put(ctx, domain.DeadLetterMessage{
			TransferID: msg.TransferID,
			Payload:    string(marshal(msg)),
			Error:      err.Error(),
			ErrorType:  "PROCESSING_ERROR",
		})

		return err
	}

	// 4. успех → PROCESSED
	return uc.inbox.Save(ctx, &domain.InboxRecord{
		TransferID: msg.TransferID,
		Status:     domain.StatusProcessed,
		Payload:    marshal(msg),
	})
}

func marshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func validate(msg domain.TransferMessage) error {
	if msg.TransferID == "" {
		return errors.New("missing transfer_id")
	}
	if msg.Amount <= 0 {
		return errors.New("invalid amount")
	}
	if msg.FromAccount == "" {
		return errors.New("missing from_account")
	}
	if msg.ToAccount == "" {
		return errors.New("missing to_account")
	}
	return nil
}
