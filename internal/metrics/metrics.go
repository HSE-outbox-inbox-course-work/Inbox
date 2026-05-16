// Package metrics держит описания всех Prometheus-метрик inbox-сервиса
// и их регистрацию в реестре. Сами места инкрементов разнесены по слоям:
//
//   - чтение из Kafka — в Listener (internal/integrations/payments/consumer);
//   - бизнес-обработка платежа — в usecase PaymentUseCase;
//   - публикация в DLQ — в DLQProducer (internal/integrations/dlq);
//   - состояние pgx-пула и таблицы inbox_order — фоновый сборщик Run().
//
// Namespace "inbox" даёт префикс inbox_* у всех имён и отделяет сервисные
// метрики от инфраструктурных (pg_*, kafka_*).
package metrics

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "inbox"

// Buckets для бизнес-длительностей: одна обработка сообщения — это пара
// DB-запросов и одна публикация в Kafka (DLQ), поэтому диапазон от долей мс
// до пары секунд достаточен.
var processingBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// e2eBuckets — задержка от момента вставки в outbox до фиксации в inbox.
// На стенде это десятки миллисекунд, но в плохом сценарии (остановка
// потребителя) задержка может расти до десятков секунд, поэтому верхний
// предел заметно шире.
var e2eBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

// Outcome — конечный набор значений метки outcome. Только перечислимые
// значения, иначе rate(...) by (outcome) даст случайные имена.
type Outcome string

const (
	OutcomeProcessed       Outcome = "processed"
	OutcomeDuplicate       Outcome = "duplicate"
	OutcomeValidationError Outcome = "validation_error"
	OutcomeProcessingError Outcome = "processing_error"
)

// ErrorType — короткий конечный список причин отправки в DLQ.
// Соответствует Inbox usecase: либо валидация, либо ошибка обработки.
type ErrorType string

const (
	ErrorTypeValidation ErrorType = "VALIDATION_ERROR"
	ErrorTypeProcessing ErrorType = "PROCESSING_ERROR"
)

// Metrics — контейнер всех коллекторов. Передаётся по указателю в те места,
// которым нужно делать наблюдения; nil-безопасных методов нет.
type Metrics struct {
	// Kafka consumer
	KafkaMessagesRead *prometheus.CounterVec // по topic+outcome (ok / decode_error)

	// Бизнес-обработка
	ProcessingDuration *prometheus.HistogramVec // по outcome
	MessagesProcessed  *prometheus.CounterVec   // по outcome

	// Дедупликация и валидация
	DuplicatesDetected *prometheus.CounterVec // по topic
	ValidationErrors   *prometheus.CounterVec // по field — какое поле упало

	// DLQ producer
	DLQMessagesProduced *prometheus.CounterVec     // по error_type + outcome (ok / send_error)
	DLQProduceDuration  *prometheus.HistogramVec   // по outcome

	// E2E latency: now - outbox.event_time, измеряется когда платёж
	// в inbox финально получает статус PROCESSED.
	DeliveryE2ELatency prometheus.Histogram

	// Состояние таблицы inbox_order (фоновый сбор)
	InboxTableRows *prometheus.GaugeVec // по status

	// pgx pool stats (фоновый сбор)
	PoolAcquired    prometheus.Gauge
	PoolIdle        prometheus.Gauge
	PoolTotal       prometheus.Gauge
	PoolMax         prometheus.Gauge
	PoolAcquireWait prometheus.Gauge
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		KafkaMessagesRead: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "kafka_messages_read_total",
			Help:      "Kafka messages read by the listener, grouped by topic and decode/read outcome.",
		}, []string{"topic", "outcome"}),

		ProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "processing_duration_seconds",
			Help:      "Duration of a single ProcessPayment call, grouped by outcome.",
			Buckets:   processingBuckets,
		}, []string{"outcome"}),

		MessagesProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_processed_total",
			Help:      "Inbox messages finalized into a terminal outcome.",
		}, []string{"outcome"}),

		DuplicatesDetected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "duplicates_total",
			Help:      "Messages skipped because inbox_order already has them with status PROCESSED.",
		}, []string{"topic"}),

		ValidationErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "validation_errors_total",
			Help:      "TransferMessage payload validation errors, partitioned by failing field.",
		}, []string{"field"}),

		DLQMessagesProduced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dlq_messages_total",
			Help:      "Messages routed to the dead-letter-queue Kafka topic.",
		}, []string{"error_type", "outcome"}),

		DLQProduceDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "dlq_produce_duration_seconds",
			Help:      "Latency of the DLQ Kafka writer's WriteMessages call.",
			Buckets:   processingBuckets,
		}, []string{"outcome"}),

		DeliveryE2ELatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "delivery_e2e_latency_seconds",
			Help:      "End-to-end delay between outbox.event_time and the moment Inbox marks a payment PROCESSED. Computed only for non-duplicate successful messages.",
			Buckets:   e2eBuckets,
		}),

		InboxTableRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "table_rows",
			Help:      "Current number of rows in the inbox_order table, partitioned by status.",
		}, []string{"status"}),

		PoolAcquired: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "pgx_pool",
			Name: "acquired_connections", Help: "pgxpool: currently acquired connections.",
		}),
		PoolIdle: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "pgx_pool",
			Name: "idle_connections", Help: "pgxpool: idle connections in the pool.",
		}),
		PoolTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "pgx_pool",
			Name: "total_connections", Help: "pgxpool: total connections currently in the pool.",
		}),
		PoolMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "pgx_pool",
			Name: "max_connections", Help: "pgxpool: configured maximum pool size.",
		}),
		PoolAcquireWait: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "pgx_pool",
			Name: "wait_count", Help: "pgxpool: cumulative number of Acquire calls that had to wait.",
		}),
	}

	reg.MustRegister(
		m.KafkaMessagesRead,
		m.ProcessingDuration, m.MessagesProcessed,
		m.DuplicatesDetected, m.ValidationErrors,
		m.DLQMessagesProduced, m.DLQProduceDuration,
		m.DeliveryE2ELatency,
		m.InboxTableRows,
		m.PoolAcquired, m.PoolIdle, m.PoolTotal, m.PoolMax, m.PoolAcquireWait,
	)
	return m
}

// Run запускает фоновый сборщик: раз в interval опрашивает pgxpool.Stat()
// и считает строки в inbox_order по статусам. Останавливается по ctx.Done().
func (m *Metrics) Run(ctx context.Context, pool *pgxpool.Pool, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	m.collectOnce(ctx, pool)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.collectOnce(ctx, pool)
		}
	}
}

func (m *Metrics) collectOnce(ctx context.Context, pool *pgxpool.Pool) {
	stat := pool.Stat()
	m.PoolAcquired.Set(float64(stat.AcquiredConns()))
	m.PoolIdle.Set(float64(stat.IdleConns()))
	m.PoolTotal.Set(float64(stat.TotalConns()))
	m.PoolMax.Set(float64(stat.MaxConns()))
	m.PoolAcquireWait.Set(float64(stat.EmptyAcquireCount()))

	// GROUP BY по status даёт три строки максимум (RECEIVED / PROCESSED / FAILED),
	// этого достаточно чтобы построить разбивку без скана payload.
	rows, err := pool.Query(ctx,
		`SELECT status, COUNT(*) FROM inbox_order GROUP BY status`)
	if err != nil {
		log.Printf("metrics: cannot collect inbox_order stats: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			log.Printf("metrics: scan inbox_order: %v", err)
			return
		}
		m.InboxTableRows.WithLabelValues(status).Set(float64(count))
	}
}
