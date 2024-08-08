package taskpool

import "errors"

var (
	ErrServiceStopped    = errors.New("service is stopped")
	ErrInvalidOption     = errors.New("invalid option")
	ErrInvalidQueueIndex = errors.New("invalid queue index")
)
