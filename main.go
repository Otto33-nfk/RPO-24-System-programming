package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8}

	sequentialResults, sequentialTime, err := measureSequential(inputs)
	if err != nil {
		fmt.Println("Ошибка последовательной обработки:", err)
		return
	}
	printResults("Последовательная обработка", sequentialResults, sequentialTime)

	oneWorkerResults, oneWorkerTime, err := measurePool(inputs, 1)
	if err != nil {
		fmt.Println("Ошибка пула из 1 обработчика:", err)
		return
	}
	printResults("Пул из 1 обработчика", oneWorkerResults, oneWorkerTime)

	fourWorkersResults, fourWorkersTime, err := measurePool(inputs, 4)
	if err != nil {
		fmt.Println("Ошибка пула из 4 обработчиков:", err)
		return
	}
	printResults("Пул из 4 обработчиков", fourWorkersResults, fourWorkersTime)

	fmt.Printf("\nУскорение пула из 1 обработчика: %.2f раза\n", float64(sequentialTime)/float64(oneWorkerTime))
	fmt.Printf("Ускорение пула из 4 обработчиков: %.2f раза\n", float64(sequentialTime)/float64(fourWorkersTime))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	partialResults, err := runPool(ctx, inputs, 4)
	fmt.Println("\nПул из 4 обработчиков с таймаутом 300 мс")
	for _, result := range partialResults {
		fmt.Printf("Число: %d, квадрат: %d\n", result.Input, result.Square)
	}
	fmt.Printf("Получено результатов: %d\n", len(partialResults))
	fmt.Println("Ошибка:", err)
}
