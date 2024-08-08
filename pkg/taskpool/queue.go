package taskpool

type Queue struct {
	Index      int
	Queue      chan Task
	Size       int
	StopSignal chan struct{}

	CloseStrategy  CloseStrategy
	PoolStopSignal chan struct{}
}

func (q *Queue) Consume() {
	for task := range q.Queue {
		task.BeforeRunHook(q)
		result := task.Run()
		task.RunFinishHook(q, result)
		task.Callback(result)
	}
}

func (q *Queue) Enqueue(task Task) {
	q.Queue <- task
}

func (q *Queue) DoClose() {
	if q.CloseStrategy == CloseStrategyWaitTask {
		q.Consume()
	} else if q.CloseStrategy == CloseStrategyDropTask {
		for task := range q.Queue {
			result := &Result{
				err: ErrServiceStopped,
			}

			task.BeforeRunHook(q)
			task.RunFinishHook(q, result)
			task.Callback(result)
		}
	}
}
