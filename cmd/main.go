package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"inbox/internal/integrations/dlq"
	kafka "inbox/internal/integrations/payments/consumer"
	"inbox/internal/metrics"
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
	metricsAddr := getenv("INBOX_METRICS_ADDR", ":8080")

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

	// Свой реестр без default-коллекторов — детерминированный /metrics,
	// без мусорных промежуточных метрик от чужих пакетов. process_* и go_*
	// добавляем явно, они нужны для USE-методологии (RAM, GC, потоки).
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)
	svcMetrics := metrics.New(reg)

	// Фоновый коллектор: pgxpool.Stat() + COUNT(*) GROUP BY status по
	// inbox_order. Интервал 5s — компромисс между свежестью данных и
	// нагрузкой на БД (запрос быстрый, table небольшой).
	go svcMetrics.Run(ctx, dbpool, 5*time.Second)

	inboxRepo := repository.NewInboxRepository(dbpool)
	processedRepo := repository.NewProcessedRepository(dbpool)

	dlqProducer := dlq.NewDLQProducer(brokers, dlqTopic, svcMetrics)

	paymentUC := usecases.NewPaymentUseCase(inboxRepo, processedRepo, dlqProducer, svcMetrics, topic)

	listener := kafka.NewListener(brokers, topic, group, paymentUC, svcMetrics)
	go listener.Listen(ctx)

	// HTTP-сервер только для /metrics. Inbox — фоновый воркер, никаких
	// бизнес-эндпойнтов у него нет; отдельный mux нужен, чтобы Prometheus
	// мог его скрейпить.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsSrv := &http.Server{
		Addr:              metricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server error: %v", err)
		}
	}()

	log.Println("inbox service started, metrics on", metricsAddr)
	<-ctx.Done()
	log.Println("shutdown signal received")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
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
