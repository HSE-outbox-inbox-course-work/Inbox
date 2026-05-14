-- +goose Up
-- +goose StatementBegin
CREATE TABLE processed_payment (
    id           BIGSERIAL    PRIMARY KEY,
    transfer_id  UUID         NOT NULL UNIQUE,
    amount       BIGINT       NOT NULL,
    from_account UUID         NOT NULL,
    to_account   UUID         NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE processed_payment;
-- +goose StatementEnd
