package repository

import (
	"context"

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
	query := `
		SELECT id, transfer_id, status, payload
		FROM inbox_order
		WHERE transfer_id = $1
		LIMIT 1
	`

	row := r.pool.QueryRow(ctx, query, id)

	var res domain.InboxRecord
	err := row.Scan(&res.ID, &res.TransferID, &res.Status, &res.Payload)
	if err != nil {
		return nil, nil
	}

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
