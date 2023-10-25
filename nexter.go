package scheduler

import (
	"time"

	"github.com/eluv-io/utc-go"
)

// Nexter is the interface of objects returning the Next UTC date of a Schedule
type Nexter interface {
	// Next returns when next notification after now must occur or utc.Zero
	Next(now utc.UTC, s *Schedule) utc.UTC
}

// NexterFn is a function implementing Nexter
type NexterFn func(now utc.UTC, s *Schedule) utc.UTC

func (f NexterFn) Next(now utc.UTC, s *Schedule) utc.UTC {
	return f(now, s)
}

// NexterTime is the interface of objects returning the Next date of a Schedule
type NexterTime interface {
	// Next returns when next notification after now must occur or the zero time
	Next(now time.Time, s *Schedule) time.Time
}

// NexterTimeFn is a function implementing NexterTime
type NexterTimeFn func(now time.Time, s *Schedule) time.Time

func (f NexterTimeFn) Next(now time.Time, s *Schedule) time.Time {
	return f(now, s)
}
