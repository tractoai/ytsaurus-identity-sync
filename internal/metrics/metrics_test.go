package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := New(registry)

	metrics.SyncStarted()
	metrics.SyncSucceeded()
	metrics.SyncPartiallySucceeded()
	metrics.ObserveSyncDuration(42 * time.Second)
	metrics.SetLastSuccessTimestamp(time.Unix(1234567890, 0))
	metrics.SetLastCompletedTimestamp(time.Unix(1234567891, 0))
	metrics.SyncFailed("users")
	metrics.YTMutationSucceeded("user", "create")
	metrics.YTMutationFailed("user", "create")
	metrics.YTMutationFailed("user", "create")
	require.Equal(t, float64(2), testutil.ToFloat64(metrics.ytMutationErrors.WithLabelValues("user", "create")))
	metrics.ResetYTMutationErrors()
	require.Zero(t, testutil.ToFloat64(metrics.ytMutationErrors.WithLabelValues("user", "create")))
	metrics.YTMutationFailed("user", "create")
	metrics.RemoveLimitExceeded("group")

	require.Equal(t, float64(1), testutil.ToFloat64(metrics.syncStarted))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.syncSuccess))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.syncPartial))
	require.Equal(t, float64(1234567890), testutil.ToFloat64(metrics.lastSuccess))
	require.Equal(t, float64(1234567891), testutil.ToFloat64(metrics.lastCompleted))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.syncErrors.WithLabelValues("users")))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.ytMutationSuccess.WithLabelValues("user", "create")))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.ytMutationErrors.WithLabelValues("user", "create")))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.removeLimitReached.WithLabelValues("group")))
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	var durationCount uint64
	var durationSum float64
	metricNames := make([]string, 0, len(metricFamilies))
	for _, family := range metricFamilies {
		metricNames = append(metricNames, family.GetName())
		if family.GetName() == "ytsaurus_identity_sync_sync_duration_seconds" {
			durationCount = family.GetMetric()[0].GetHistogram().GetSampleCount()
			durationSum = family.GetMetric()[0].GetHistogram().GetSampleSum()
		}
	}
	require.Equal(t, uint64(1), durationCount)
	require.Equal(t, float64(42), durationSum)
	require.ElementsMatch(t, []string{
		"ytsaurus_identity_sync_sync_started_total",
		"ytsaurus_identity_sync_sync_success_total",
		"ytsaurus_identity_sync_sync_partial_total",
		"ytsaurus_identity_sync_sync_duration_seconds",
		"ytsaurus_identity_sync_last_success_timestamp_seconds",
		"ytsaurus_identity_sync_last_completed_timestamp_seconds",
		"ytsaurus_identity_sync_sync_errors_total",
		"ytsaurus_identity_sync_yt_mutation_success_total",
		"ytsaurus_identity_sync_yt_mutation_errors",
		"ytsaurus_identity_sync_remove_limit_exceeded_total",
	}, metricNames)
}
