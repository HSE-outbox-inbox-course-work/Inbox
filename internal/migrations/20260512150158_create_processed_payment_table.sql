-- +goose Up
CREATE TABLE processed_payment (
                                   id BIGSERIAL PRIMARY KEY,
                                   transfer_id UUID NOT NULL UNIQUE,
                                   amount BIGINT NOT NULL,
                                   from_account UUID NOT NULL,
                                   to_account UUID NOT NULL,
);
-- +goose Down
SELECT 'down SQL query';
