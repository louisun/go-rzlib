package taskpool

import "time"

type ServiceStatus int

const (
	StatusRunning ServiceStatus = 1
	StatusStopped ServiceStatus = -1
)

type CloseStrategy int

const (
	CloseStrategyWaitTask CloseStrategy = iota
	CloseStrategyDropTask
)

type QueueSelector func(key string, queueNum int) int

type Result struct {
	err error
	msg string
	ext map[string]interface{}
}

func DefaultQueueSelector(_ string, queueNum int) int {
	return int(time.Now().UnixMilli() % int64(queueNum))
}
