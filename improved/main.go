package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Task struct {
	id      int
	data    string
	retries int
}

const maxRetries = 3

func worker(id int, taskChan <-chan Task, wg *sync.WaitGroup, failChan chan<- Task) {
	defer wg.Done()
	for task := range taskChan {
		fmt.Printf("[Worker %d] Started task %d: %s (Retry: %d)\n", id, task.id, task.data, task.retries)

		if rand.Float32() < 0.3 {
			fmt.Printf("[Worker %d] Failed task %d\n", id, task.id)
			failChan <- task
			continue
		}
		
		time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second)
		fmt.Printf("[Worker %d] Completed task %d\n", id, task.id)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	var tasks = make([]Task, 5)
	for i := 1; i < 6; i++ {
		tasks[i-1] = Task{id: i, data: fmt.Sprintf("Task %d", i), retries: 0}
	}

	taskChan := make(chan Task, len(tasks))
	failChan := make(chan Task, len(tasks))

	var wg sync.WaitGroup
	numWorkers := 3
	counter := 0
	
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, taskChan, &wg, failChan)
	}

	go func() {
		for _, task := range tasks {
			taskChan <- task 
			counter = (counter + 1) % numWorkers
		}
		close(taskChan) 
	}()

	go func() {
		for failedTask := range failChan {
			if failedTask.retries < maxRetries {
				failedTask.retries++
				fmt.Printf("[Failure Handler] Reassigning task %d (Retry: %d)\n", failedTask.id, failedTask.retries)
				taskChan <- failedTask 
			} else {
				fmt.Printf("[Failure Handler] Task %d reached maximum retries, logging failure.\n", failedTask.id)
			}
		}
	}()

	wg.Wait()
	close(failChan)

	fmt.Println("All tasks completed.")
}
