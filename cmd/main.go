package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"inbox/internal/integrations/dlq"
	kafka "inbox/internal/integrations/payments/consumer"
	repository "inbox/internal/repo"
	"inbox/internal/usecases"
)

const migrationsPath = "internal/migrations"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	dsn := getenv("INBOX_DB_DSN",
		"postgres://postgres:postgres@localhost:5433/inbox?sslmode=disable")
	brokers := []string{getenv("INBOX_KAFKA_BROKER", "localhost:29092")}
	topic := getenv("INBOX_KAFKA_TOPIC", "accounts.money.transferred")
	group := getenv("INBOX_KAFKA_GROUP", "inbox-service")
	dlqTopic := getenv("INBOX_DLQ_TOPIC", "dead-letter-queue")

	dbpool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db connection error: %v", err)
	}
	defer dbpool.Close()

	if err := dbpool.Ping(ctx); err != nil {
		log.Fatalf("db ping error: %v", err)
	}

	if err := runMigrations(dbpool); err != nil {
		log.Fatalf("migrations failed: %v", err)
	}

	inboxRepo := repository.NewInboxRepository(dbpool)
	processedRepo := repository.NewProcessedRepository(dbpool)

	dlqProducer := dlq.NewDLQProducer(brokers, dlqTopic)

	paymentUC := usecases.NewPaymentUseCase(inboxRepo, processedRepo, dlqProducer)

	listener := kafka.NewListener(brokers, topic, group, paymentUC)
	go listener.Listen(ctx)

	log.Println("inbox service started")
	<-ctx.Done()
	log.Println("shutdown signal received")
}

func runMigrations(pool *pgxpool.Pool) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.Up(db, migrationsPath)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
