package wpn

import (
	"context"
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

type JobFunc func(ctx context.Context) error
