package taskpool

import (
	"context"
	"time"
)

type Task interface {
	// Key returns the key of the task for queue selection
	Key() string
	// Run the task and return the result
	Run() *Result
	// Callback the task result
	Callback(result *Result)
	// GetTaskInfo returns the queue selector for the task
	GetTaskInfo() TaskInfo

	TaskHook
}

type TaskInfo struct {
	Ctx      context.Context
	Selector QueueSelector
	Ext      map[string]any
}

type TaskHook interface {
	// BeforeSubmitHook is called before the task is submitted to the pool
	BeforeSubmitHook(p TaskPool, q *Queue)
	// BeforeRunHook is called before the task is consumed by the queue
	BeforeRunHook(q *Queue)
	// RunFinishHook is called after the task is finished
	RunFinishHook(q *Queue, result *Result)
}

type BasicTask struct {
	// Basic Info
	Ctx      context.Context
	Selector QueueSelector
	Ext      map[string]any

	// Time info
	SubmitTime time.Time
	RunTime    time.Time
	FinishTime time.Time

	// Duration info
	WaitDuration  time.Duration
	RunDuration   time.Duration
	TotalDuration time.Duration
}

func (b *BasicTask) Key() string {
	return ""
}

func (b *BasicTask) Run() *Result {
	return nil
}

func (b *BasicTask) Callback(_ *Result) {
}

func (b *BasicTask) BeforeSubmitHook(p TaskPool, q *Queue) {
	b.SubmitTime = time.Now()
}

func (b *BasicTask) BeforeRunHook(_ *Queue) {
	b.RunTime = time.Now()
	b.WaitDuration = b.RunTime.Sub(b.SubmitTime)
}

func (b *BasicTask) RunFinishHook(_ *Queue, _ *Result) {
	b.FinishTime = time.Now()
	b.RunDuration = b.FinishTime.Sub(b.RunTime)
	b.TotalDuration = b.FinishTime.Sub(b.SubmitTime)
}

func (b *BasicTask) GetTaskInfo() TaskInfo {
	return TaskInfo{
		Ctx:      b.Ctx,
		Selector: b.Selector,
		Ext:      b.Ext,
	}
}
