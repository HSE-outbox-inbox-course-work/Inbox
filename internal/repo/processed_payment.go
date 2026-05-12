package repository

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"inbox/internal/domain"
)

type ProcessedRepository struct {
	pool *pgxpool.Pool
}

func NewProcessedRepository(pool *pgxpool.Pool) *ProcessedRepository {
	return &ProcessedRepository{pool: pool}
}

func (r *ProcessedRepository) Create(ctx context.Context, p *domain.ProcessedPayment) error {
	query := `
		INSERT INTO processed_payment (
			transfer_id,
			amount,
			from_account,
			to_account
		)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (transfer_id) DO NOTHING
	`

	_, err := r.pool.Exec(ctx, query,
		p.TransferID,
		p.Amount,
		p.FromAccount,
		p.ToAccount,
	)

	return err
}
