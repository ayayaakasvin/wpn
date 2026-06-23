// Package provides custom job query that is aimed to create maximum observablity of processes and logging.
package wpn

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
)

type Sumbitter interface {
	Submit(JobFunc, context.Context) error
}

type WorkerPool struct {
	ctx    context.Context
	cancel context.CancelFunc

	jobsChan   chan *Job
	resultChan chan *Result

	wg        *sync.WaitGroup
	submitWg  sync.WaitGroup
	mu        sync.Mutex
	started   bool
	shutdown  bool
	closeOnce sync.Once

	workerCount int
	busyWorkers atomic.Int64

	processedJobs atomic.Int64
	failedJobs    atomic.Int64
	retriedJobs   atomic.Int64
}

func NewWorkerPool(workerCount int) (*WorkerPool, error) {
	if workerCount < 1 {
		return nil, fmt.Errorf("worker count is less than 1: %d", workerCount)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wp := &WorkerPool{
		ctx:        ctx,
		cancel:     cancel,
		jobsChan:   make(chan *Job, 64),
		resultChan: make(chan *Result, 128),

		wg:          new(sync.WaitGroup),
		workerCount: workerCount,
	}

	return wp, nil
}

func (wp *WorkerPool) Stats() *Stats {
	st := &Stats{
		WorkerCount: wp.workerCount,
		BusyWorkers: int(wp.busyWorkers.Load()),
		QueueLength: len(wp.jobsChan),

		ProcessedJobs: int(wp.processedJobs.Load()),
		FailedJobs:    int(wp.failedJobs.Load()),
		RetriedJobs:   int(wp.retriedJobs.Load()),
	}

	if total := st.ProcessedJobs + st.FailedJobs; total > 0 {
		st.MissRate = (float64(st.FailedJobs) / float64(total)) * 100
	}

	return st
}

func (wp *WorkerPool) Submit(jfn JobFunc, ctx context.Context) error {
	if jfn == nil {
		return fmt.Errorf("job function is nil")
	}

	if ctx == nil {
		return fmt.Errorf("ctx is nil")
	}

	wp.mu.Lock()
	if !wp.started {
		wp.mu.Unlock()
		return fmt.Errorf("worker pool has not been started")
	}
	if wp.shutdown {
		wp.mu.Unlock()
		return fmt.Errorf("worker pool has been shut down")
	}
	wp.submitWg.Add(1)
	wp.mu.Unlock()
	defer wp.submitWg.Done()

	job := &Job{
		ID:      ulid.Make().String(),
		Context: ctx,
		Exec:    jfn,
	}

	select {
	case wp.jobsChan <- job:
		return nil
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool has been shut down")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (wp *WorkerPool) Results() <-chan *Result {
	return wp.resultChan
}

func (wp *WorkerPool) Start(parent context.Context) error {
	if wp.workerCount < 1 {
		return fmt.Errorf("worker count is less than 1: %d", wp.workerCount)
	}

	wp.mu.Lock()
	defer wp.mu.Unlock()

	if wp.started {
		return fmt.Errorf("worker pool already started")
	}

	ctx, cancel := context.WithCancel(parent)
	wp.ctx = ctx
	wp.cancel = cancel
	wp.started = true

	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	return nil
}

func (wp *WorkerPool) Shutdown(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	wp.mu.Lock()
	if !wp.started || wp.shutdown {
		wp.mu.Unlock()
		return
	}
	wp.shutdown = true
	wp.mu.Unlock()

	wp.submitWg.Wait()

	wp.closeOnce.Do(func() {
		close(wp.jobsChan)
	})

	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		wp.cancel()
		<-done
	}

	close(wp.resultChan)
}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case job, ok := <-wp.jobsChan:
			if !ok {
				return
			}

			wp.busyWorkers.Add(1)
			defer wp.busyWorkers.Add(-1)

			start := time.Now()
			var lastErr error
			attempts := 0

			for i := 1; i <= defaultRetryAttempt; i++ {
				attempts = i
				lastErr = job.Exec(job.Context)

				if lastErr == nil {
					wp.processedJobs.Add(1)
					break
				}

				if i < defaultRetryAttempt {
					wp.retriedJobs.Add(1)
				}
			}

			finish := time.Now()
			res := &Result{
				JobID:        job.ID,
				Output:       OutputSuccessful,
				StartedAt:    start,
				FinishedAt:   finish,
				TimeConsumed: finish.Sub(start),
				Attempts:     attempts,
				WorkerID:     id,
				Error:        nil,
			}

			if lastErr != nil {
				res.Output = OutputFail
				res.Error = lastErr
				wp.failedJobs.Add(1)
			}

			wp.resultChan <- res
		}
	}
}
