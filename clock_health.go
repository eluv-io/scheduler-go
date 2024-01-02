package scheduler

import (
	"time"

	"github.com/eluv-io/utc-go"
)

// ClockHealth allows checking the system clock is in sync with the internal
// monotonic clock, for example in cases where the user puts the system to sleep.
type ClockHealth struct {
	Enabled      bool
	MaxClockSkew time.Duration
	PollPeriod   time.Duration
}

// The clockHealth checks the system clock is in sync with the internal monotonic clock.
// See https://github.com/golang/go/issues/35012
//
// A clock skew can be caused by:
//  1. The system clock being adjusted
//     -> this eg. happens when ntp adjusts the system clock
//  2. Pausing the process (e.g. with SIGSTOP)
//     -> the monotonic clock will stop, but the system clock will continue
//     -> this eg. happens when you pause a VM/ hibernate a laptop
//
// Small clock skews - less than maxClockSkew are allowed, because they can happen when
// the system clock is adjusted.
// However, we do compound the clock skew over time, so that if the clock skew
// is small but constant, it will eventually fail the health check.
type clockHealth struct {
	scheduler     *Scheduler
	stop          chan struct{}
	maxClockSkew  time.Duration
	pollPeriod    time.Duration
	timeReal      utc.UTC
	timeMonotonic utc.UTC
	skew          time.Duration
}

func newClockHealth(scheduler *Scheduler, opts ClockHealth) *clockHealth {
	maxClockSkew := opts.MaxClockSkew
	if maxClockSkew < time.Second {
		maxClockSkew = time.Second
	}
	now := utc.Now()
	return &clockHealth{
		scheduler:     scheduler,
		stop:          make(chan struct{}),
		maxClockSkew:  maxClockSkew,
		pollPeriod:    opts.PollPeriod,
		timeReal:      now.Round(0), // .Round(0) removes the monotonic part from the time
		timeMonotonic: now,
	}
}

func (c *clockHealth) timeSkewed() bool {
	now := utc.Now()
	realDuration := now.Sub(c.timeReal)
	monotonicDuration := now.Sub(c.timeMonotonic)

	totalSkew := (realDuration - monotonicDuration).Abs()
	skew := totalSkew - c.skew
	if skew < 0 {
		skew = 0
	}
	//c.scheduler.logger.Trace("time skew", "skew", skew)
	if skew >= c.maxClockSkew {
		c.scheduler.logger.Debug("time skewed",
			"skew", skew,
			"max_clock_skew", c.maxClockSkew)
		c.skew = totalSkew
		return true
	}
	return false
}

func (c *clockHealth) halt() {
	close(c.stop)
}

func (c *clockHealth) run() {
	if c.pollPeriod <= 0 {
		return
	}
	ticker := time.NewTicker(c.pollPeriod)

	go func() {
		for {
			select {
			case <-c.stop:
				return
			case <-ticker.C:
				if c.timeSkewed() {
					c.scheduler.reset <- struct{}{}
				}
			}
		}
	}()

}
