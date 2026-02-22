package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	ErrInvalidSize = errors.New("worker is <= 0")
)

// Job represents the task to be executed by worker
type Job struct {
	ID string
}

// WorkerPool represents a pool of worker goroutines
type WorkerPool struct {
	workers  int
	jobQueue chan Job
	results  chan string
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	once     sync.Once
}

func NewWorkerPool(ctx context.Context, numWorkers int) (*WorkerPool, error) {
	if numWorkers <= 0 {
		return nil, ErrInvalidSize
	}
	ctx, cancel := context.WithCancel(ctx)
	return &WorkerPool{
		workers:  numWorkers,
		jobQueue: make(chan Job, 10),
		results:  make(chan string, 10),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// worker to process jobs
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()
	for {
		select {
		case j, ok := <-wp.jobQueue:
			if !ok {
				return // channel closed
			}
			fmt.Printf("worker %v: started job %v\n", id, j.ID)
			time.Sleep(500 * time.Millisecond)
			fmt.Printf("worker %v: finished job %v\n", id, j.ID)
			wp.results <- j.ID
		case <-wp.ctx.Done():
			fmt.Printf("worker %v: context cancelled\n", id)
			return

		}
	}
}

// Start the worker pool distribute jobs to workers

func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

func (wp *WorkerPool) Wait() {
	wp.once.Do(func() {
		close(wp.jobQueue)
		wp.wg.Wait()
		close(wp.results)
		wp.cancel()
	})
}

// Submit a job to the jobQueue
func (wp *WorkerPool) Submit(job Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pool is shut down")
		}
	}()
	wp.jobQueue <- job
	return nil
}

func (wp *WorkerPool) CollectResults() {
	for r := range wp.results {
		fmt.Printf("Result received for job %v\n", r)
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	numWorkers := 10
	workerPool, err := NewWorkerPool(ctx, numWorkers)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	workerPool.Start()
	done := make(chan struct{})
	go func() {
		workerPool.CollectResults()
		close(done)
	}()
	for i := 0; i < numWorkers; i++ {
		workerPool.Submit(Job{ID: fmt.Sprintf("job-%d", i)})
	}
	workerPool.Wait()
	<-done
	fmt.Println("shutdown complete")
}
