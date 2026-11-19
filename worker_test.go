package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunPoolResults(t *testing.T) {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8}

	results, err := runPool(context.Background(), inputs, 4)
	if err != nil {
		t.Fatalf("runPool вернул ошибку: %v", err)
	}

	if len(results) != 8 {
		t.Fatalf("ожидалось 8 результатов, получено %d", len(results))
	}

	for i, result := range results {
		wantInput := i + 1
		wantSquare := wantInput * wantInput

		if result.Input != wantInput || result.Square != wantSquare {
			t.Errorf("результат %d: получено {%d, %d}, ожидалось {%d, %d}",
				i, result.Input, result.Square, wantInput, wantSquare)
		}
	}
}

func TestRunPoolCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results, err := runPool(ctx, []int{1, 2, 3, 4}, 2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ожидалась ошибка context.Canceled, получено %v", err)
	}

	if len(results) != 0 {
		t.Errorf("ожидалось 0 результатов, получено %d", len(results))
	}
}

func TestRunPoolInvalidWorkers(t *testing.T) {
	_, err := runPool(context.Background(), []int{1, 2, 3}, 0)
	if err == nil {
		t.Fatal("ожидалась ошибка при workers = 0")
	}
}

func TestRunPoolTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := runPool(ctx, []int{1, 2, 3, 4}, 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ожидалась ошибка context.DeadlineExceeded, получено %v", err)
	}
}
