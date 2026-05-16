package stream

import "errors"

var (
	ErrStreamClosed      = errors.New("stream closed by client")
	ErrBlockTimeout      = errors.New("block fetch timed out")
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
	ErrPipeDrained       = errors.New("pipe drained")
)
