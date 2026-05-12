package cmd

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"inbox/internal/integrations/dlq"
	"inbox/internal/integrations/payments/consumer"
	"inbox/internal/repo"
	"inbox/internal/usecases"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// --- DB ---
	dbpool, err := pgxpool.New(ctx, "postgres://user:password@localhost:5432/app?sslmode=disable")
	if err != nil {
		log.Fatal("db connection error:", err)
	}
	defer dbpool.Close()

	// --- repositories ---
	inboxRepo := repository.NewInboxRepository(dbpool)
	processedRepo := repository.NewProcessedRepository(dbpool)

	// --- DLQ (простая заглушка, потом можно заменить на Kafka topic) ---
	dlqProducer := dlq.NewDLQProducer(
		[]string{"localhost:9092"},
		"dead-letter-queue",
	)
	// --- usecase ---
	paymentUC := usecases.NewPaymentUseCase(
		inboxRepo,
		processedRepo,
		dlqProducer,
	)

	// --- kafka listener ---
	listener := kafka.NewListener(
		[]string{"localhost:9092"},
		"accounts.money.transferred",
		"inbox-service",
		paymentUC,
	)

	// run kafka in background
	go listener.Listen(ctx)

	log.Println("service started")

	// wait shutdown
	<-sigCh
	log.Println("shutdown signal received")

	cancel()
}
