-- +goose Up
-- +goose StatementBegin
CREATE TABLE inbox_order (
    id          BIGSERIAL    PRIMARY KEY,
    transfer_id UUID         NOT NULL UNIQUE,
    status      VARCHAR(20)  NOT NULL DEFAULT 'RECEIVED',
    payload     JSONB        NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE inbox_order;
-- +goose StatementEnd
