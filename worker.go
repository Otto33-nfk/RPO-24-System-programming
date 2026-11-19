package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Result struct {
	Input  int
	Square int
}

func processOne(ctx context.Context, input int) (Result, error) {
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C:
		return Result{Input: input, Square: input * input}, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func runSequential(ctx context.Context, inputs []int) ([]Result, error) {
	results := make([]Result, 0, len(inputs))

	for _, input := range inputs {
		result, err := processOne(ctx, input)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}

	return results, nil
}

func runPool(ctx context.Context, inputs []int, workers int) ([]Result, error) {
	if workers < 1 {
		return nil, errors.New("число обработчиков должно быть не меньше 1")
	}

	jobs := make(chan int)
	results := make(chan Result)
	producerDone := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(workers)

	go func() {
		defer close(producerDone)
		defer close(jobs)

		for _, input := range inputs {
			select {
			case jobs <- input:
			case <-ctx.Done():
				return
			}
		}
	}()

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case input, ok := <-jobs:
					if !ok {
						return
					}

					result, err := processOne(ctx, input)
					if err != nil {
						return
					}

					select {
					case results <- result:
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

	collected := make([]Result, 0, len(inputs))
	for result := range results {
		collected = append(collected, result)
	}

	<-producerDone

	sort.Slice(collected, func(i, j int) bool {
		return collected[i].Input < collected[j].Input
	})

	if err := ctx.Err(); err != nil {
		return collected, err
	}

	return collected, nil
}

func medianDuration(times []time.Duration) time.Duration {
	sort.Slice(times, func(i, j int) bool {
		return times[i] < times[j]
	})
	return times[len(times)/2]
}

func measureSequential(inputs []int) ([]Result, time.Duration, error) {
	times := make([]time.Duration, 3)
	var results []Result

	for i := 0; i < 3; i++ {
		start := time.Now()
		current, err := runSequential(context.Background(), inputs)
		times[i] = time.Since(start)
		if err != nil {
			return nil, 0, err
		}
		results = current
	}

	return results, medianDuration(times), nil
}

func measurePool(inputs []int, workers int) ([]Result, time.Duration, error) {
	times := make([]time.Duration, 3)
	var results []Result

	for i := 0; i < 3; i++ {
		start := time.Now()
		current, err := runPool(context.Background(), inputs, workers)
		times[i] = time.Since(start)
		if err != nil {
			return nil, 0, err
		}
		results = current
	}

	return results, medianDuration(times), nil
}

func printResults(name string, results []Result, duration time.Duration) {
	fmt.Println("\n" + name)

	sum := 0
	for _, result := range results {
		fmt.Printf("Число: %d, квадрат: %d\n", result.Input, result.Square)
		sum += result.Square
	}

	fmt.Printf("Количество результатов: %d\n", len(results))
	fmt.Printf("Сумма квадратов: %d\n", sum)
	if len(results) == 8 && sum == 204 {
		fmt.Println("Проверка: успешно")
	} else {
		fmt.Println("Проверка: ошибка")
	}
	fmt.Printf("Медианное время: %v\n", duration)
}
