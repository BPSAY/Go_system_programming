package main

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

var testInputs = []int{1, 2, 3, 4, 5, 6, 7, 8}

func wantPairs() []Result {
	return []Result{
		{Input: 1, Square: 1}, {Input: 2, Square: 4},
		{Input: 3, Square: 9}, {Input: 4, Square: 16},
		{Input: 5, Square: 25}, {Input: 6, Square: 36},
		{Input: 7, Square: 49}, {Input: 8, Square: 64},
	}
}

func checkAllPairs(t *testing.T, got []Result) {
	t.Helper()
	sortResults(got)
	want := wantPairs()
	if len(got) != len(want) {
		t.Fatalf("получили %d результатов, ожидали %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("пара %d: получили %+v, ожидали %+v", i, got[i], want[i])
		}
	}
	if s := sumSquares(got); s != 204 {
		t.Errorf("сумма квадратов = %d, ожидали 204", s)
	}
}

func TestRunPoolFullFourWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := runPool(ctx, testInputs, 4)
	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	checkAllPairs(t, got)
}

func TestRunPoolFullOneWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := runPool(ctx, testInputs, 1)
	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	checkAllPairs(t, got)
}

func TestRunSequentialFull(t *testing.T) {
	got, err := runSequential(context.Background(), testInputs)
	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	checkAllPairs(t, got)
	for i, r := range got {
		if r.Input != testInputs[i] {
			t.Errorf("позиция %d: Input=%d, ожидали %d", i, r.Input, testInputs[i])
		}
	}
}

func TestRunPoolInvalidWorkers(t *testing.T) {
	for _, w := range []int{0, -1, -8} {
		got, err := runPool(context.Background(), testInputs, w)
		if !errors.Is(err, ErrInvalidWorkers) {
			t.Errorf("workers=%d: err=%v, ожидали ErrInvalidWorkers", w, err)
		}
		if got != nil {
			t.Errorf("workers=%d: ожидали nil-результаты, получили %+v", w, got)
		}
	}
}

func TestRunPoolCancelledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	var (
		got []Result
		err error
	)
	go func() {
		defer close(done)
		got, err = runPool(ctx, testInputs, 4)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runPool зависел на заранее отменённом контексте")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, ожидали context.Canceled", err)
	}
	if len(got) != 0 {
		t.Fatalf("ожидали 0 результатов, получили %+v", got)
	}
}

func TestRunPoolTimeoutPartial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	got, err := runPool(ctx, testInputs, 4)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, ожидали context.DeadlineExceeded", err)
	}
	if len(got) == 0 || len(got) >= len(testInputs) {
		t.Fatalf("ожидали частичный результат, получили %d: %+v", len(got), got)
	}
	if elapsed > time.Second {
		t.Errorf("возврат занял слишком много времени: %v", elapsed)
	}
	seen := make(map[int]bool)
	for _, r := range got {
		if r.Square != r.Input*r.Input || r.Input < 1 || r.Input > 8 || seen[r.Input] {
			t.Errorf("некорректная пара в частичном результате: %+v", r)
		}
		seen[r.Input] = true
	}
}

func TestRunPoolNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	_, err := runPool(ctx, testInputs, 4)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ожидали таймаут, получили %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		after := runtime.NumGoroutine()
		if after <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("утечка горутин: было %d, стало %d", before, after)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestProcessOneCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if _, err := processOne(ctx, 5); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, ожидали context.Canceled", err)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("processOne не прервался вовремя: %v", d)
	}
}
