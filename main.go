package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

const workDelay = 200 * time.Millisecond

var ErrInvalidWorkers = errors.New("число обработчиков должно быть не меньше 1")

type Result struct {
	Input  int
	Square int
}

func processOne(ctx context.Context, n int) (Result, error) {
	timer := time.NewTimer(workDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return Result{Input: n, Square: n * n}, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func runSequential(ctx context.Context, inputs []int) ([]Result, error) {
	out := make([]Result, 0, len(inputs))
	for _, n := range inputs {
		r, err := processOne(ctx, n)
		if err != nil {
			return out, err
		}
		out = append(out, r)
	}
	return out, nil
}

func runPool(ctx context.Context, inputs []int, workers int) ([]Result, error) {
	if workers < 1 {
		return nil, fmt.Errorf("%w: получено %d", ErrInvalidWorkers, workers)
	}

	jobs := make(chan int)
	results := make(chan Result)

	go func() {
		defer close(jobs)
		for _, n := range inputs {
			select {
			case jobs <- n:
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case n, ok := <-jobs:
					if !ok {
						return
					}
					r, err := processOne(ctx, n)
					if err != nil {
						return
					}
					select {
					case results <- r:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	out := make([]Result, 0, len(inputs))
	for r := range results {
		out = append(out, r)
	}

	if err := ctx.Err(); err != nil {
		return out, err
	}
	return out, nil
}

func sortResults(rs []Result) {
	sort.Slice(rs, func(i, j int) bool { return rs[i].Input < rs[j].Input })
}

func sumSquares(rs []Result) int {
	s := 0
	for _, r := range rs {
		s += r.Square
	}
	return s
}

func inputsOf(rs []Result) []int {
	out := make([]int, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Input)
	}
	return out
}

func median(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), ds...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return (cp[mid-1] + cp[mid]) / 2
}

func ratio(base, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(base) / float64(d)
}

type mode struct {
	name    string
	workers int
	timeout time.Duration
}

type measurement struct {
	results []Result
	err     error
	elapsed time.Duration
}

func measureOnce(inputs []int, m mode) measurement {
	ctx := context.Background()
	if m.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.timeout)
		defer cancel()
	}

	start := time.Now()
	var (
		rs  []Result
		err error
	)
	if m.workers == 0 {
		rs, err = runSequential(ctx, inputs)
	} else {
		rs, err = runPool(ctx, inputs, m.workers)
	}
	return measurement{results: rs, err: err, elapsed: time.Since(start)}
}

func main() {
	runs := flag.Int("runs", 3, "число повторов каждого режима")
	flag.Parse()
	if *runs < 1 {
		fmt.Fprintln(os.Stderr, "-runs должен быть >= 1")
		os.Exit(2)
	}

	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8}
	modes := []mode{
		{"Последовательно", 0, 0},
		{"Пул: 1 обработчик", 1, 0},
		{"Пул: 4 обработчика", 4, 0},
		{"Пул: 4, таймаут 300мс", 4, 300 * time.Millisecond},
	}

	fmt.Printf("Заданий: %d, ожидание на задание: %v, повторов: %d\n\n", len(inputs), workDelay, *runs)

	var seqMedian time.Duration
	ok := true

	for _, m := range modes {
		durations := make([]time.Duration, 0, *runs)
		var last measurement
		for i := 1; i <= *runs; i++ {
			last = measureOnce(inputs, m)
			durations = append(durations, last.elapsed)
			fmt.Printf("%-22s прогон %d: %7v | результатов %d | сумма %3d | err=%v\n",
				m.name, i, last.elapsed.Round(time.Millisecond),
				len(last.results), sumSquares(last.results), last.err)
		}

		med := median(durations)
		if m.workers == 0 {
			seqMedian = med
		}

		sorted := append([]Result(nil), last.results...)
		sortResults(sorted)

		fmt.Printf("%-22s медиана %v | ускорение %.2fx\n", m.name, med.Round(time.Millisecond), ratio(seqMedian, med))
		fmt.Printf("%-22s порядок получения: %v\n", "", inputsOf(last.results))
		fmt.Printf("%-22s по возрастанию:    %v\n\n", "", sorted)

		if m.timeout == 0 {
			if last.err != nil || len(last.results) != len(inputs) || sumSquares(last.results) != 204 {
				fmt.Fprintf(os.Stderr, "ПРОВАЛ: режим %q должен дать 8 результатов и сумму 204\n", m.name)
				ok = false
			}
		} else {
			if !errors.Is(last.err, context.DeadlineExceeded) || len(last.results) >= len(inputs) {
				fmt.Fprintf(os.Stderr, "ПРОВАЛ: режим %q должен дать частичный результат и таймаут\n", m.name)
				ok = false
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	rs, err := runPool(ctx, inputs, 4)
	fmt.Printf("Заранее отменённый контекст: результатов %d, err=%v, время %v\n",
		len(rs), err, time.Since(start).Round(time.Microsecond))
	if !errors.Is(err, context.Canceled) || len(rs) != 0 {
		ok = false
	}

	if !ok {
		os.Exit(1)
	}
}
