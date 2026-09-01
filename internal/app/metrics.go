package app

import "time"

const (
	syncStageUsers  = "users"
	syncStageGroups = "groups"

	objectTypeUser       = "user"
	objectTypeGroup      = "group"
	objectTypeMembership = "membership"

	mutationCreate = "create"
	mutationUpdate = "update"
	mutationRemove = "remove"
	mutationBan    = "ban"
	mutationAdd    = "add"
)

type Metrics interface {
	SyncStarted()
	SyncSucceeded()
	SyncPartiallySucceeded()
	ObserveSyncDuration(duration time.Duration)
	SetLastSuccessTimestamp(timestamp time.Time)
	SetLastCompletedTimestamp(timestamp time.Time)
	SyncFailed(stage string)
	YTMutationSucceeded(objectType, operation string)
	YTMutationFailed(objectType, operation string)
	ResetYTMutationErrors()
	RemoveLimitExceeded(objectType string)
}

type noopMetrics struct{}

func (noopMetrics) SyncStarted()                        {}
func (noopMetrics) SyncSucceeded()                      {}
func (noopMetrics) SyncPartiallySucceeded()             {}
func (noopMetrics) ObserveSyncDuration(time.Duration)   {}
func (noopMetrics) SetLastSuccessTimestamp(time.Time)   {}
func (noopMetrics) SetLastCompletedTimestamp(time.Time) {}
func (noopMetrics) SyncFailed(string)                   {}
func (noopMetrics) YTMutationSucceeded(string, string)  {}
func (noopMetrics) YTMutationFailed(string, string)     {}
func (noopMetrics) ResetYTMutationErrors()              {}
func (noopMetrics) RemoveLimitExceeded(string)          {}

func (a *App) metricsRecorder() Metrics {
	if a.metrics == nil {
		return noopMetrics{}
	}
	return a.metrics
}
