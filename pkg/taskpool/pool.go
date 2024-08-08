package taskpool

import (
	"context"
	"github.com/pkg/errors"
	"sync"
)

type TaskPool interface {
	Start() error
	Stop()
	Submit(ctx context.Context, task Task) error
}

type DefaultTaskPool struct {
	Option   PoolOption
	Queues   []*Queue
	Selector QueueSelector
	PoolStatus
}

type PoolOption struct {
	PoolSize      int
	QueueSize     int
	CloseStrategy CloseStrategy
}

type PoolStatus struct {
	Status     ServiceStatus
	StopSignal chan struct{}
	StopOnce   sync.Once
}

func NewTaskPool(option PoolOption) (*DefaultTaskPool, error) {
	if option.PoolSize <= 0 {
		return nil, errors.Wrapf(ErrInvalidOption, "PoolSize must be greater than 0, but got %d", option.PoolSize)
	}

	if option.QueueSize <= 0 {
		return nil, errors.Wrapf(ErrInvalidOption, "QueueSize must be greater than 0, but got %d", option.QueueSize)
	}

	if option.CloseStrategy != CloseStrategyDropTask && option.CloseStrategy != CloseStrategyWaitTask {
		return nil, errors.Wrapf(ErrInvalidOption, "CloseStrategy invalid")

	}

	pool := &DefaultTaskPool{
		Option: PoolOption{},
		Queues: make([]*Queue, option.PoolSize),
	}

	for idx := 0; idx < option.PoolSize; idx++ {
		q := &Queue{
			Index: idx,
			Queue: make(chan Task, option.QueueSize),
			Size:  option.QueueSize,
		}

		pool.Queues[idx] = q
	}

	pool.Status = StatusRunning

	return pool, nil
}

func (p *DefaultTaskPool) GetOption() *PoolOption {
	return &p.Option
}

func (p *DefaultTaskPool) Start() error {
	for _, q := range p.Queues {
		go q.Consume()
	}

	return nil
}

func (p *DefaultTaskPool) Stop() {
	p.StopOnce.Do(func() {
		p.Status = StatusStopped
		p.StopSignal <- struct{}{}
	})

	wg := sync.WaitGroup{}
	wg.Add(p.Option.PoolSize)
	for _, queue := range p.Queues {
		q := queue

		go func() {
			defer wg.Done()
			if p.Option.CloseStrategy == CloseStrategyWaitTask {
				q.Consume()
			} else if p.Option.CloseStrategy == CloseStrategyDropTask {
				for task := range q.Queue {
					result := &Result{
						err: ErrServiceStopped,
					}
					task.BeforeRunHook(q)
					task.RunFinishHook(q, result)
					task.Callback(result)
				}
			}

			close(q.Queue)
		}()
	}

	wg.Wait()
}

func (p *DefaultTaskPool) Submit(ctx context.Context, task Task) error {
	if p.Status != StatusRunning {
		return ErrServiceStopped
	}

	selector := task.GetTaskInfo().Selector
	if selector == nil {
		selector = p.Selector
	}

	if selector == nil {
		selector = DefaultQueueSelector
	}

	idx := p.Selector(task.Key(), p.Option.QueueSize)
	if idx <= 0 || idx > p.Option.PoolSize {
		return errors.Wrapf(ErrInvalidQueueIndex, "Selector return invalid index %d", idx)
	}

	if p.Status != StatusRunning {
		return ErrServiceStopped
	}

	q := p.Queues[idx]
	task.BeforeSubmitHook(p, q)
	q.Enqueue(task)
	return nil
}

func (p *DefaultTaskPool) SetDefaultSelector(selector QueueSelector) {
	p.Selector = selector
}
