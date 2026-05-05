-- +goose Up
CREATE TABLE inbox_payment (
         id BIGSERIAL PRIMARY KEY,
         order_id BIGINT NOT NULL,
         status VARCHAR(20) NOT NULL DEFAULT 'RECEIVED'
);
//todo добавить таблицу payment