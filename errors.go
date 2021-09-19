package szBinary

import "errors"

var (
	// ErrUnexpectedEOF occurs when no enough data to read by codec.
	errUnexpectedEOF = errors.New("there is no enough data")

	errBuffLenthExceed = errors.New("exceeding buffer length")

	errNegativeLength = errors.New("negative length is invalid")

	errNotConnected = errors.New("client not connected")

	errIsRunning = errors.New("task is already running")

	errIsNotRunning = errors.New("task is not running")

	errEmptyPartitions = errors.New("partitions is empty")
)
