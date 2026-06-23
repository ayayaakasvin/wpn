// Package provides custom job query that is aimed to create maximum observablity of processes and logging.
package wpn

import (
	"context"
	"fmt"
	"sync"
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
	closeOnce sync.Once

	workerCount int
}

func NewWorkerPool(workerCount int) (*WorkerPool, error) {
	if workerCount < 1 {
		return nil, fmt.Errorf("worker count is less than 1: %d", workerCount)
	}

	wp := &WorkerPool{
		jobsChan:   make(chan *Job, 64),
		resultChan: make(chan *Result, 128),

		wg:          new(sync.WaitGroup),
		workerCount: workerCount,
	}

	return wp, nil
}

func (wp *WorkerPool) Submit(jfn JobFunc, ctx context.Context) (err error) {
	if jfn == nil {
		return fmt.Errorf("job function is nil")
	}

	job := &Job{
		ID:      ulid.Make().String(),
		Context: ctx,
		Exec:    jfn,
	}

	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case error:
				if v.Error() == "send on closed channel" {
					err = fmt.Errorf("worker pool has been shut down")
					return
				}
			case string:
				if v == "send on closed channel" {
					err = fmt.Errorf("worker pool has been shut down")
					return
				}
			}
			panic(r)
		}
	}()

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
	ctx, cancel := context.WithCancel(parent)

	wp.ctx = ctx
	wp.cancel = cancel

	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	return nil
}

func (wp *WorkerPool) Shutdown(ctx context.Context) {
	wp.cancel()

	// close job channel once, wait for workers to finish, then close results
	wp.closeOnce.Do(func() {
		close(wp.jobsChan)
	})
	wp.wg.Wait()
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

			start := time.Now()

			res := &Result{
				JobID:     job.ID,
				Output:    OutputSuccessful,
				StartedAt: start,
				WorkerID:  id,
				Error:     nil,
			}

			for i := 1; i <= defaultRetryAttempt; i++ {
				err := job.Exec(job.Context)
				res.Attempts = i

				if err != nil {
					res.Output = OutputFail
					res.Error = err
				} else {
					res.Output = OutputSuccessful
					res.Error = nil
					break
				}
			}

			finish := time.Now()
			res.FinishedAt = finish
			res.TimeConsumed = finish.Sub(start)
			wp.resultChan <- res
		}
	}
}
