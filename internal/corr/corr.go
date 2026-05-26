package corr

import (
	"fmt"
	"sync/atomic"
	"time"
)

type CorrelationToken string

var counter atomic.Uint64

func New() CorrelationToken {
	nanos := time.Now().UnixNano()
	n := counter.Add(1)
	return CorrelationToken(fmt.Sprintf("%020d-%020d", nanos, n))
}

func Compare(a, b CorrelationToken) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
