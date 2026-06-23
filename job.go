package wpn

import (
	"context"
	"time"
)

var (
	defaultRetryAttempt = 3
)

func SetRetryAttempts(ra int) {
	if ra < 0 {
		return
	}

	defaultRetryAttempt = ra
}

type Job struct {
	ID      string
	Context context.Context
	Exec    JobFunc
}

type Result struct {
	// Job identification
	JobID string

	// Output produced by the job (if any). Kept as int for backward
	// compatibility with existing code; change as needed.
	Output Output

	// Error, if the job failed.
	Error error

	// Timing and progress metrics
	StartedAt    time.Time
	FinishedAt   time.Time
	TimeConsumed time.Duration

	// Execution details
	Attempts int // number of attempts/retries
	WorkerID int // identifier of the worker that ran the job

	// Arbitrary additional metrics (e.g., memory, CPU). Values are
	// application-defined.
	// Metrics map[string]float64
}

type JobFunc func(ctx context.Context) error
