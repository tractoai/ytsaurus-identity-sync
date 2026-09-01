package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "ytsaurus_identity_sync"

// Metrics contains Prometheus collectors for synchronization outcomes.
type Metrics struct {
	syncStarted        prometheus.Counter
	syncSuccess        prometheus.Counter
	syncPartial        prometheus.Counter
	syncDuration       prometheus.Histogram
	lastSuccess        prometheus.Gauge
	lastCompleted      prometheus.Gauge
	syncErrors         *prometheus.CounterVec
	ytMutationSuccess  *prometheus.CounterVec
	ytMutationErrors   *prometheus.GaugeVec
	removeLimitReached *prometheus.CounterVec
}

// New creates and registers application metrics in registerer.
func New(registerer prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		syncStarted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "sync_started_total",
			Help:      "Total number of synchronization iterations started.",
		}),
		syncSuccess: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "sync_success_total",
			Help:      "Total number of synchronization iterations completed without errors.",
		}),
		syncPartial: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "sync_partial_total",
			Help:      "Total number of synchronization iterations completed with one or more mutation errors.",
		}),
		syncDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "sync_duration_seconds",
			Help:      "Duration of synchronization iterations in seconds.",
			Buckets:   []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800},
		}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "last_success_timestamp_seconds",
			Help:      "Unix timestamp of the last successful synchronization iteration.",
		}),
		lastCompleted: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "last_completed_timestamp_seconds",
			Help:      "Unix timestamp of the last fully or partially completed synchronization iteration.",
		}),
		syncErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "sync_errors_total",
			Help:      "Total number of failed synchronization stages.",
		}, []string{"stage"}),
		ytMutationSuccess: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "yt_mutation_success_total",
			Help:      "Total number of successfully completed YTsaurus mutation operations.",
		}, []string{"object_type", "operation"}),
		ytMutationErrors: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "yt_mutation_errors",
			Help:      "Number of failed YTsaurus mutation operations in the current synchronization iteration.",
		}, []string{"object_type", "operation"}),
		removeLimitReached: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "remove_limit_exceeded_total",
			Help:      "Total number of synchronization iterations stopped by the removal limit.",
		}, []string{"object_type"}),
	}

	registerer.MustRegister(
		metrics.syncStarted,
		metrics.syncSuccess,
		metrics.syncPartial,
		metrics.syncDuration,
		metrics.lastSuccess,
		metrics.lastCompleted,
		metrics.syncErrors,
		metrics.ytMutationSuccess,
		metrics.ytMutationErrors,
		metrics.removeLimitReached,
	)
	return metrics
}

func (m *Metrics) SyncStarted() {
	m.syncStarted.Inc()
}

func (m *Metrics) SyncSucceeded() {
	m.syncSuccess.Inc()
}

func (m *Metrics) SyncPartiallySucceeded() {
	m.syncPartial.Inc()
}

func (m *Metrics) ObserveSyncDuration(duration time.Duration) {
	m.syncDuration.Observe(duration.Seconds())
}

func (m *Metrics) SetLastSuccessTimestamp(timestamp time.Time) {
	m.lastSuccess.Set(float64(timestamp.Unix()))
}

func (m *Metrics) SetLastCompletedTimestamp(timestamp time.Time) {
	m.lastCompleted.Set(float64(timestamp.Unix()))
}

func (m *Metrics) SyncFailed(stage string) {
	m.syncErrors.WithLabelValues(stage).Inc()
}

func (m *Metrics) YTMutationSucceeded(objectType, operation string) {
	m.ytMutationSuccess.WithLabelValues(objectType, operation).Inc()
}

func (m *Metrics) YTMutationFailed(objectType, operation string) {
	m.ytMutationErrors.WithLabelValues(objectType, operation).Inc()
}

func (m *Metrics) ResetYTMutationErrors() {
	m.ytMutationErrors.Reset()
}

func (m *Metrics) RemoveLimitExceeded(objectType string) {
	m.removeLimitReached.WithLabelValues(objectType).Inc()
}
