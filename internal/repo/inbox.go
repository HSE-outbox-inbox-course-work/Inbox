package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"inbox/internal/domain"
)

type InboxRepository struct {
	pool *pgxpool.Pool
}

func NewInboxRepository(pool *pgxpool.Pool) *InboxRepository {
	return &InboxRepository{pool: pool}
}

func (r *InboxRepository) GetByTransferID(ctx context.Context, id string) (*domain.InboxRecord, error) {
	// transfer_id::text и status::text — pgx v5 без явной регистрации
	// типов не сканирует Postgres UUID и enum-подобные VARCHAR в Go
	// string и в типизированные string-alias'ы (как domain.InboxStatus).
	// Кастуем в text на стороне БД и сканируем в обычный string;
	// status потом приводим к InboxStatus уже в Go.
	query := `
		SELECT id, transfer_id::text, status::text, payload
		FROM inbox_order
		WHERE transfer_id = $1
		LIMIT 1
	`

	var (
		res    domain.InboxRecord
		status string
	)
	err := r.pool.QueryRow(ctx, query, id).Scan(&res.ID, &res.TransferID, &status, &res.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	res.Status = domain.InboxStatus(status)
	return &res, nil
}

func (r *InboxRepository) Save(ctx context.Context, msg *domain.InboxRecord) error {
	query := `
		INSERT INTO inbox_order (transfer_id, status, payload)
		VALUES ($1, $2, $3)
		ON CONFLICT (transfer_id)
		DO UPDATE SET status = EXCLUDED.status, payload = EXCLUDED.payload
	`

	_, err := r.pool.Exec(ctx, query,
		msg.TransferID,
		msg.Status,
		msg.Payload,
	)

	return err
}
