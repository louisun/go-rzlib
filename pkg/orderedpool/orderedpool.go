package orderedpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

// Task represents a task to be executed by the pool
type Task struct {
	Key  string      // Key identifies related tasks that need sequential processing
	Data interface{} // Data for the task
}

// ProcessFn is the function signature for task processing
type ProcessFn func(ctx context.Context, task Task) error

// ErrorHandler is the function signature for handling errors
type ErrorHandler func(task Task, err error)

// OrderedPool is a goroutine pool that processes tasks with the same key in sequential order
type OrderedPool struct {
	maxWorkers   int                // Maximum number of key workers allowed
	processFn    ProcessFn          // Function to process tasks
	errorHandler ErrorHandler       // Function to handle errors
	wg           sync.WaitGroup     // WaitGroup for all workers
	ctx          context.Context    // Context for the pool
	cancel       context.CancelFunc // Function to cancel the pool's context
	taskCh       chan Task          // Channel for incoming tasks
	idleTimeout  time.Duration      // Timeout for idle queues

	// Stats and status tracking
	taskCount int64        // Atomic counter for tasks (positive: pending, negative: completed)
	isClosed  bool         // Flag indicating if the pool is closed
	closeMu   sync.RWMutex // Using RWMutex instead of Mutex for better read performance

	// For each key, maintain a dedicated processing goroutine
	keyWorkers     map[string]*keyWorker // Workers for each key
	keyWorkersMu   sync.RWMutex          // Already using RWMutex, no change needed
	submitTimeout  time.Duration         // Timeout for task submission to key channels
	keyChannelSize int                   // Size of each key's dedicated channel buffer
}

// keyWorker contains all info related to processing tasks for a specific key
type keyWorker struct {
	taskCh     chan Task    // Channel for tasks with this key
	lastAccess atomic.Value // Last time this worker was accessed (stores time.Time)
}

// New creates a new OrderedPool with the specified number of workers and process function
func New(maxWorkers int, processFn ProcessFn, options ...Option) (*OrderedPool, error) {
	// Validate input parameters
	if maxWorkers <= 0 {
		return nil, errors.New("maxWorkers must be greater than 0")
	}
	if processFn == nil {
		return nil, errors.New("processFn cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &OrderedPool{
		maxWorkers:     maxWorkers,
		keyWorkers:     make(map[string]*keyWorker),
		processFn:      processFn,
		ctx:            ctx,
		cancel:         cancel,
		taskCh:         make(chan Task, maxWorkers*10), // Buffer size is 10x number of workers
		idleTimeout:    5 * time.Minute,                // Default idle timeout
		submitTimeout:  3 * time.Second,                // Default submission timeout
		errorHandler:   defaultErrorHandler,            // Default error handler
		keyChannelSize: 100,                            // Default key channel buffer size
	}

	// Apply options
	for _, option := range options {
		if err := option(pool); err != nil {
			cancel() // Clean up context if option application fails
			return nil, err
		}
	}

	// Start the dispatcher - responsible for routing tasks to corresponding key channels
	go pool.dispatcher()

	// Start worker cleaner
	go pool.workerCleaner()

	return pool, nil
}

// defaultErrorHandler is the default function for handling errors
func defaultErrorHandler(task Task, err error) {
	// By default, we do nothing with errors
	// Users should provide their own error handler if needed
}

// Option is a function type to apply optional configurations to the pool
type Option func(*OrderedPool) error

// WithIdleTimeout sets the idle timeout for workers
func WithIdleTimeout(duration time.Duration) Option {
	return func(p *OrderedPool) error {
		if duration <= 0 {
			return errors.New("idle timeout must be greater than 0")
		}
		p.idleTimeout = duration
		return nil
	}
}

// WithSubmitTimeout sets the timeout for task submission to key channels
func WithSubmitTimeout(duration time.Duration) Option {
	return func(p *OrderedPool) error {
		if duration <= 0 {
			return errors.New("submit timeout must be greater than 0")
		}
		p.submitTimeout = duration
		return nil
	}
}

// WithErrorHandler sets a custom error handler function
func WithErrorHandler(handler ErrorHandler) Option {
	return func(p *OrderedPool) error {
		if handler == nil {
			return errors.New("error handler cannot be nil")
		}
		p.errorHandler = handler
		return nil
	}
}

// WithKeyChannelSize sets the buffer size for each key's channel
func WithKeyChannelSize(size int) Option {
	return func(p *OrderedPool) error {
		if size <= 0 {
			return errors.New("key channel size must be greater than 0")
		}
		p.keyChannelSize = size
		return nil
	}
}

// Submit adds a task to the pool
func (p *OrderedPool) Submit(task Task) error {
	// Validate task first to reduce errors before acquiring locks
	if task.Key == "" {
		return errors.New("task key cannot be empty")
	}

	// Check if pool is closed - using read lock for better performance
	p.closeMu.RLock()
	if p.isClosed {
		p.closeMu.RUnlock()
		return errors.New("pool is closed")
	}
	p.closeMu.RUnlock()

	// Check if context is cancelled early to avoid unnecessary processing
	select {
	case <-p.ctx.Done():
		return errors.New("pool is closed")
	default:
		// Continue execution
	}

	// Increment pending tasks counter
	atomic.AddInt64(&p.taskCount, 1)

	// Try to submit the task
	select {
	case <-p.ctx.Done():
		atomic.AddInt64(&p.taskCount, -1) // Decrement counter if not submitted
		return errors.New("pool is closed")
	case p.taskCh <- task:
		return nil
	case <-time.After(p.submitTimeout):
		atomic.AddInt64(&p.taskCount, -1) // Decrement counter if not submitted
		return errors.New("submission timeout: pool might be overloaded")
	}
}

// SubmitWithTimeout adds a task to the pool with a custom timeout
func (p *OrderedPool) SubmitWithTimeout(task Task, timeout time.Duration) error {
	// Validate task first to reduce errors before acquiring locks
	if task.Key == "" {
		return errors.New("task key cannot be empty")
	}

	if timeout <= 0 {
		return errors.New("timeout must be greater than 0")
	}

	// Check if pool is closed - using read lock for better performance
	p.closeMu.RLock()
	if p.isClosed {
		p.closeMu.RUnlock()
		return errors.New("pool is closed")
	}
	p.closeMu.RUnlock()

	// Check if context is cancelled early to avoid unnecessary processing
	select {
	case <-p.ctx.Done():
		return errors.New("pool is closed")
	default:
		// Continue execution
	}

	// Increment pending tasks counter
	atomic.AddInt64(&p.taskCount, 1)

	// Try to submit the task with the custom timeout
	select {
	case <-p.ctx.Done():
		atomic.AddInt64(&p.taskCount, -1) // Decrement counter if not submitted
		return errors.New("pool is closed")
	case p.taskCh <- task:
		return nil
	case <-time.After(timeout):
		atomic.AddInt64(&p.taskCount, -1) // Decrement counter if not submitted
		return errors.Errorf("submission timeout after %v: pool might be overloaded", timeout)
	}
}

// Close stops all workers and waits for them to finish
func (p *OrderedPool) Close() {
	p.closeMu.Lock()
	if p.isClosed {
		p.closeMu.Unlock()
		return // Already closed
	}
	p.isClosed = true
	p.closeMu.Unlock()

	p.cancel() // Signal all goroutines to stop

	// Close all dedicated key channels
	p.keyWorkersMu.Lock()
	for _, worker := range p.keyWorkers {
		close(worker.taskCh)
	}
	p.keyWorkersMu.Unlock()

	p.wg.Wait() // Wait for all processors to finish
}

// GracefulClose stops the pool and waits for all submitted tasks to be processed
func (p *OrderedPool) GracefulClose(timeout time.Duration) error {
	// First mark the pool as closed to prevent new submissions
	p.closeMu.Lock()
	if p.isClosed {
		p.closeMu.Unlock()
		return nil // Already closed
	}
	p.isClosed = true
	p.closeMu.Unlock()

	// Create a context with timeout for waiting
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Use a more efficient waiting method to avoid frequent checking
	doneCh := make(chan struct{})
	go func() {
		for {
			pendingCount := atomic.LoadInt64(&p.taskCount)
			if pendingCount <= 0 {
				close(doneCh)
				return // All tasks completed
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait for completion or timeout
	select {
	case <-ctx.Done():
		// Timeout reached, force close
		p.cancel()
		p.wg.Wait()
		pendingCount := atomic.LoadInt64(&p.taskCount)
		return errors.Errorf("graceful close timed out with %d pending tasks", pendingCount)
	case <-doneCh:
		// All tasks completed, now close normally
		p.cancel()
		p.wg.Wait()
		return nil
	}
}

// Stats returns statistics about the pool
func (p *OrderedPool) Stats() map[string]interface{} {
	p.keyWorkersMu.RLock()
	defer p.keyWorkersMu.RUnlock()

	pendingCount := atomic.LoadInt64(&p.taskCount)
	completedCount := int64(0)
	if pendingCount < 0 {
		// This shouldn't happen in normal operation
		completedCount = -pendingCount
		pendingCount = 0
	} else {
		// We've completed more tasks than the pending count indicates
		completedCount = p.getCompletedCount()
	}

	// Check isClosed status using read lock
	p.closeMu.RLock()
	isClosed := p.isClosed
	p.closeMu.RUnlock()

	return map[string]interface{}{
		"active_keys":     len(p.keyWorkers),
		"pending_tasks":   pendingCount,
		"completed_tasks": completedCount,
		"is_closed":       isClosed,
	}
}

// getCompletedCount calculates completed tasks based on the taskCount
// (negative values in taskCount indicate completed tasks beyond pending ones)
func (p *OrderedPool) getCompletedCount() int64 {
	taskCount := atomic.LoadInt64(&p.taskCount)
	if taskCount < 0 {
		return -taskCount
	}
	// If no negative count, we can't determine completed count
	return 0
}

// dispatcher routes tasks to the corresponding key channel
func (p *OrderedPool) dispatcher() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case task := <-p.taskCh:
			// Use a function to encapsulate handling logic, reducing code complexity in the main loop
			p.dispatchTask(task)
		}
	}
}

// dispatchTask handles the dispatching of a single task
func (p *OrderedPool) dispatchTask(task Task) {
	// Early check if context is cancelled
	select {
	case <-p.ctx.Done():
		atomic.AddInt64(&p.taskCount, -1) // Decrement counter for unsent task
		return
	default:
		// Continue execution
	}

	// Get or create dedicated channel for the key
	worker := p.getOrCreateKeyWorker(task.Key)

	// Send the task to the corresponding key channel with timeout
	select {
	case <-p.ctx.Done():
		atomic.AddInt64(&p.taskCount, -1) // Decrement counter for unsent task
	case worker.taskCh <- task:
		// Task routed successfully
		// Update last access time
		worker.lastAccess.Store(time.Now())
	case <-time.After(p.submitTimeout):
		// Handle submission timeout
		atomic.AddInt64(&p.taskCount, -1) // Decrement counter for unsent task
		if p.errorHandler != nil {
			p.errorHandler(task, errors.New("key channel submission timeout"))
		}
	}
}

// getOrCreateKeyWorker gets or creates a dedicated worker for a key
func (p *OrderedPool) getOrCreateKeyWorker(key string) *keyWorker {
	// Try to get with read lock first, as most cases the key already exists
	p.keyWorkersMu.RLock()
	worker, exists := p.keyWorkers[key]
	p.keyWorkersMu.RUnlock()

	if exists {
		return worker
	}

	// Create new worker
	p.keyWorkersMu.Lock()
	defer p.keyWorkersMu.Unlock()

	// Double-check to avoid race condition
	worker, exists = p.keyWorkers[key]
	if exists {
		return worker
	}

	// Create worker and start dedicated processing goroutine
	worker = &keyWorker{
		taskCh: make(chan Task, p.keyChannelSize),
	}
	worker.lastAccess.Store(time.Now())
	p.keyWorkers[key] = worker

	// Start dedicated processor for this key
	p.wg.Add(1)
	go p.keyProcessor(key, worker)

	return worker
}

// keyProcessor processes all tasks for a specific key
func (p *OrderedPool) keyProcessor(key string, worker *keyWorker) {
	defer p.wg.Done()

	// Use a cached variable to reduce the overhead of repeatedly getting lastAccess
	var lastTaskProcessTime time.Time

	for {
		select {
		case <-p.ctx.Done():
			// Process remaining tasks in the channel before exiting
			p.drainChannel(worker)
			return
		case task, ok := <-worker.taskCh:
			if !ok {
				// Channel closed
				return
			}

			// Reduce frequency of atomic.Value writes by not updating lastAccess too often
			currentTime := time.Now()
			// Only update lastAccess when more than 100ms has passed since the last update
			if currentTime.Sub(lastTaskProcessTime) > 100*time.Millisecond {
				worker.lastAccess.Store(currentTime)
				lastTaskProcessTime = currentTime
			}

			// Process task - sequential order guaranteed by dedicated goroutine for each key
			startTime := time.Now()
			err := p.processFn(p.ctx, task)
			processingTime := time.Since(startTime)

			// Decrement pending count and increment completed (with one atomic operation)
			atomic.AddInt64(&p.taskCount, -2) // -1 for pending, -1 for completed (negative values)

			// Similarly reduce frequency of atomic.Value writes
			currentTime = time.Now()
			if currentTime.Sub(lastTaskProcessTime) > 100*time.Millisecond {
				worker.lastAccess.Store(currentTime)
				lastTaskProcessTime = currentTime
			}

			// Handle errors if any
			if err != nil && p.errorHandler != nil {
				p.errorHandler(task, err)
			}

			// Add some breathing room to prevent tight loops
			if processingTime < time.Millisecond {
				time.Sleep(time.Millisecond)
			}
		}
	}
}

// drainChannel processes any remaining tasks in a channel after the context is cancelled
func (p *OrderedPool) drainChannel(worker *keyWorker) {
	// Create a separate context for drain processing
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()

	for {
		select {
		case <-drainCtx.Done():
			// Timeout reached for draining
			return
		case task, ok := <-worker.taskCh:
			if !ok {
				// Channel is closed and empty
				return
			}

			// Process the task
			p.processFn(drainCtx, task)

			// Update counters
			atomic.AddInt64(&p.taskCount, -2) // -1 for pending, -1 for completed (negative values)
		default:
			// No more tasks in channel
			return
		}
	}
}

// workerCleaner periodically removes idle workers
func (p *OrderedPool) workerCleaner() {
	ticker := time.NewTicker(p.idleTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.cleanIdleWorkers()
		}
	}
}

// cleanIdleWorkers removes idle key workers
func (p *OrderedPool) cleanIdleWorkers() {
	now := time.Now()

	// Avoid frequent write lock acquisition until idle workers are confirmed
	// Use separate collection and deletion operations to reduce lock contention
	idleKeysMap := make(map[string]*keyWorker)

	// First identify idle workers - using read lock
	p.keyWorkersMu.RLock()
	for key, worker := range p.keyWorkers {
		// Check if worker is idle
		lastAccess, ok := worker.lastAccess.Load().(time.Time)
		if !ok {
			continue // Invalid type, skip
		}

		// Only consider it idle if the channel is empty
		if now.Sub(lastAccess) > p.idleTimeout && len(worker.taskCh) == 0 {
			idleKeysMap[key] = worker
		}
	}
	p.keyWorkersMu.RUnlock()

	// If we found idle workers, remove them
	if len(idleKeysMap) > 0 {
		p.keyWorkersMu.Lock()
		for key, worker := range idleKeysMap {
			// Double-check that worker still exists in map (may have been removed after read lock was released)
			if currentWorker, exists := p.keyWorkers[key]; exists && currentWorker == worker {
				// Double-check that it's still idle
				lastAccess, ok := worker.lastAccess.Load().(time.Time)
				if !ok {
					continue
				}

				if now.Sub(lastAccess) > p.idleTimeout && len(worker.taskCh) == 0 {
					close(worker.taskCh) // This will cause the keyProcessor to exit
					delete(p.keyWorkers, key)
				}
			}
		}
		p.keyWorkersMu.Unlock()
	}
}
