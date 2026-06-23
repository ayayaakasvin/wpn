package wpn_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ayayaakasvin/wpn"
)

func TestNewWorkerPoolValidation(t *testing.T) {
	_, err := wpn.NewWorkerPool(0)
	if err == nil {
		t.Fatal("expected error for worker count < 1")
	}
}

func TestWorkerPoolSubmitNilJobFunc(t *testing.T) {
	wp, err := wpn.NewWorkerPool(1)
	if err != nil {
		t.Fatalf("unexpected NewWorkerPool error: %v", err)
	}

	if err := wp.Submit(nil, context.Background()); err == nil {
		t.Fatal("expected error for nil job function")
	}
}

func TestWorkerPoolExecSuccess(t *testing.T) {
	wp, err := wpn.NewWorkerPool(2)
	if err != nil {
		t.Fatalf("unexpected NewWorkerPool error: %v", err)
	}

	if err := wp.Start(context.Background()); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	defer wp.Shutdown(context.Background())

	jobCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := wp.Submit(func(ctx context.Context) error { return nil }, jobCtx); err != nil {
		t.Fatalf("unexpected Submit error: %v", err)
	}

	select {
	case res := <-wp.Results():
		if res.Output != wpn.OutputSuccessful {
			t.Fatalf("expected successful output, got %v", res.Output)
		}
		if res.Error != nil {
			t.Fatalf("expected no error, got %v", res.Error)
		}
		if res.JobID == "" {
			t.Fatal("expected job ID to be set")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

func TestWorkerPoolExecFail(t *testing.T) {
	wp, err := wpn.NewWorkerPool(1)
	if err != nil {
		t.Fatalf("unexpected NewWorkerPool error: %v", err)
	}

	if err := wp.Start(context.Background()); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	defer wp.Shutdown(context.Background())

	errFail := errors.New("failure")
	if err := wp.Submit(func(ctx context.Context) error { return errFail }, context.Background()); err != nil {
		t.Fatalf("unexpected Submit error: %v", err)
	}

	select {
	case res := <-wp.Results():
		if res.Output != wpn.OutputFail {
			t.Fatalf("expected fail output, got %v", res.Output)
		}
		if !errors.Is(res.Error, errFail) {
			t.Fatalf("expected error %v, got %v", errFail, res.Error)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for failed result")
	}
}

func TestWorkerPoolRetryAttempts(t *testing.T) {
	prevRetryAttempts := 3
	wpn.SetRetryAttempts(2)
	defer wpn.SetRetryAttempts(prevRetryAttempts)

	wp, err := wpn.NewWorkerPool(1)
	if err != nil {
		t.Fatalf("unexpected NewWorkerPool error: %v", err)
	}

	if err := wp.Start(context.Background()); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	defer wp.Shutdown(context.Background())

	attempts := 0
	errFail := errors.New("transient failure")
	if err := wp.Submit(func(ctx context.Context) error {
		attempts++
		if attempts < 2 {
			return errFail
		}
		return nil
	}, context.Background()); err != nil {
		t.Fatalf("unexpected Submit error: %v", err)
	}

	select {
	case res := <-wp.Results():
		if res.Output != wpn.OutputSuccessful {
			t.Fatalf("expected successful output after retry, got %v", res.Output)
		}
		if res.Error != nil {
			t.Fatalf("expected no error after successful retry, got %v", res.Error)
		}
		if res.Attempts != 2 {
			t.Fatalf("expected 2 attempts, got %d", res.Attempts)
		}
		if attempts != 2 {
			t.Fatalf("expected job executed 2 times, got %d", attempts)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

func TestWorkerPoolSubmitAfterShutdown(t *testing.T) {
	wp, err := wpn.NewWorkerPool(1)
	if err != nil {
		t.Fatalf("unexpected NewWorkerPool error: %v", err)
	}

	if err := wp.Start(context.Background()); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	wp.Shutdown(context.Background())

	if err := wp.Submit(func(ctx context.Context) error { return nil }, context.Background()); err == nil {
		t.Fatal("expected error after worker pool shutdown")
	}
}

func TestOutputString(t *testing.T) {
	cases := []struct {
		value    wpn.Output
		expected string
	}{
		{wpn.OutputFail, "Failed"},
		{wpn.OutputSuccessful, "Completed"},
		{wpn.Output(999), "Unknown result"},
	}

	for _, c := range cases {
		if got := c.value.String(); got != c.expected {
			t.Errorf("Output(%d).String() = %q, want %q", c.value, got, c.expected)
		}
	}
}
