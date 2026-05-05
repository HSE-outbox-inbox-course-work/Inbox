package repo

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

type InboxStatus string

const (
	StatusReceived  InboxStatus = "RECEIVED"
	StatusProcessed InboxStatus = "PROCESSED"
	StatusFailed    InboxStatus = "FAILED"
)

type InboxOrder struct {
	ID        int64
	OrderID   int64
	EventType string
	Payload   json.RawMessage
	Status    InboxStatus
}

type InboxRepository struct {
	pool *pgxpool.Pool
}

func NewInboxRepository(pool *pgxpool.Pool) *InboxRepository {
	return &InboxRepository{pool: pool}
}

func (r *InboxRepository) Create(ctx context.Context, msg *InboxOrder) error {
	query := `
		INSERT INTO inbox_order (order_id, event_type, payload, status)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`

	return r.pool.QueryRow(
		ctx,
		query,
		msg.OrderID,
		msg.EventType,
		msg.Payload,
		msg.Status,
	).Scan(&msg.ID)
}

func (r *InboxRepository) GetUnprocessed(ctx context.Context, limit int) ([]InboxOrder, error) {
	query := `
		SELECT id, order_id, event_type, payload, status
		FROM inbox_order
		WHERE status = $1
		ORDER BY id
		LIMIT $2
	`

	rows, err := r.pool.Query(ctx, query, StatusReceived, limit)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []InboxOrder

	for rows.Next() {
		var msg InboxOrder

		err := rows.Scan(
			&msg.ID,
			&msg.OrderID,
			&msg.EventType,
			&msg.Payload,
			&msg.Status,
		)
		if err != nil {
			return nil, err
		}

		result = append(result, msg)
	}

	return result, rows.Err()
}

func (r *InboxRepository) MarkProcessed(ctx context.Context, id int64) error {
	cmdTag, err := r.pool.Exec(ctx,
		`UPDATE inbox_order SET status = $1 WHERE id = $2`,
		StatusProcessed,
		id,
	)
	if err != nil {
		return err
	}

	if cmdTag.RowsAffected() == 0 {
		return errors.New("message not found")
	}

	return nil
}

func (r *InboxRepository) MarkFailed(ctx context.Context, id int64) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE inbox_order SET status = $1 WHERE id = $2`,
		StatusFailed,
		id,
	)
	return err
}
