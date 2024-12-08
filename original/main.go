package main 

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

)

type Task struct {
	id int
	data string
}

func worker(id int, taskChan <- chan  Task, wg *sync.WaitGroup, failChan chan<- Task){
	defer wg.Done()
	for task := range taskChan {
		fmt.Printf("Worker %d satrted task %d: %s\n", id, task.id, task.data);

		if rand.Float32() < 0.3{
			fmt.Printf("Worker %d failed on task %d\n",id, task.id);
			failChan <- task 
			return 
		}
		time.Sleep(time.Duration(rand.Intn(3) + 1) * time.Second)
		fmt.Printf("Worker %d completed task %d\n", id, task.id);
	}
}


func main() {
	rand.Seed(time.Now().UnixNano())

	var tasks = make([]Task, 5)
	for i := 1; i < 6; i++ {
		tasks[i - 1] = Task{id: i, data: fmt.Sprintf("Task %d", i)}
	}
	
	
	taskChan := make(chan Task, len(tasks))
	failChan := make(chan Task, len(tasks))

	var wg sync.WaitGroup 
	numWorkers := 3

	for i:= 1; i < numWorkers + 1; i ++ {
		wg.Add(1)
		go worker(i, taskChan, &wg, failChan)
			
	}

	for _, task := range tasks {
		taskChan <- task 
	}
	close(taskChan)

	go func(){
		for failedTask := range failChan {
			fmt.Printf("Reassigning failed task %d\n", failedTask.id)
			go worker(rand.Intn(numWorkers) + 1, taskChan, &wg, failChan)
		}
	}()

	wg.Wait()
	close(failChan)

	fmt.Println("All tasks completed")

}


