package clock

import (
	"sync"
	"time"
)

// Clock is a monotonically increasing ticker.
type Clock struct {
	last int64
	mut  sync.Mutex
}

// Tick returns the next clock tick. It defaults to UnixNano() but will always
// return a time at that least one nanosecond later than the previous
// invocation. Tick() is safe to call from multiple goroutines.
func (c *Clock) Tick() int64 {
	c.mut.Lock()
	defer c.mut.Unlock()

	cur := time.Now().UnixNano()
	if cur > c.last {
		c.last = cur
	} else {
		c.last++
	}
	return c.last
}

var defaultClock Clock

// Tick is a convenience function that returns the next tick of the default
// clock.
func Tick() int64 {
	return defaultClock.Tick()
}
