package scheduler

import (
	"time"

	"github.com/robfig/cron/v3"

	"github.com/eluv-io/errors-go"
	"github.com/eluv-io/utc-go"
)

// ScheduleOpt is a configuration function of Schedule. The predefined variables
// Occur, Recur and Until provide such functions.
type ScheduleOpt func(s *Schedule) error

func scheduleErr(op, err string) error {
	return errors.NoTrace(op, errors.K.Invalid, "reason", err)
}

var (
	Occur = occur{} // Occur has functions to give an initial date to a Schedule
	Recur = recur{} // Recur has functions to configure a recurrent Schedule
	Until = until{} // Until has functions to configure termination of a recurrent schedule
)

type occur struct{}

// At specifies when the schedule will be fired.
func (occur) At(t utc.UTC) ScheduleOpt {
	return func(s *Schedule) error {
		if !s.next.IsZero() {
			return scheduleErr("at", "multiple 'at'")
		}
		if s.starter != nil {
			return scheduleErr("at", "conflict 'at' and 'in'")
		}
		s.next = t
		return nil
	}
}

// In specifies a duration after which the schedule must be fired. The exact date
// is computed by the scheduler when the schedule is received.
func (occur) In(d time.Duration) ScheduleOpt {
	return func(s *Schedule) error {
		if s.starter != nil {
			return scheduleErr("in", "multiple 'in'")
		}
		if !s.next.IsZero() {
			return scheduleErr("in", "conflict 'in' and 'at'")
		}
		s.starter = NexterFn(func(now utc.UTC, _ *Schedule) utc.UTC { return now.Add(d) })
		return nil
	}
}

type recur struct{}

// Next uses a `Nexter` object to compute the next firing date.
func (recur) Next(n Nexter) ScheduleOpt {
	return func(s *Schedule) error {
		if s.nexter != nil {
			return scheduleErr("next", "multiple recurring options")
		}
		s.nexter = n
		return nil
	}
}

// Every makes the schedule fire every given duration.
func (recur) Every(d time.Duration) ScheduleOpt {
	return func(s *Schedule) error {
		if s.nexter != nil {
			return scheduleErr("every", "multiple recurring options")
		}
		s.nexter = NexterFn(func(now utc.UTC, _ *Schedule) utc.UTC { return now.Add(d) })
		return nil
	}
}

// NextTime uses a function to compute the next time the schedule must fire.
func (recur) NextTime(fn NexterTime) ScheduleOpt {
	return func(s *Schedule) error {
		if s.nexter != nil {
			return scheduleErr("nextTime", "multiple recurring options")
		}
		s.nexter = NexterFn(func(now utc.UTC, s *Schedule) utc.UTC {
			return utc.New(fn.Next(now.Time, s))
		})
		return nil
	}
}

// Cron returns configure a schedule with a cron/v3 Schedule
// Example:
//
//	s, _ := cron.ParseStandard("5 * * * *") // every 5 minutes
//	opt := Recur.Cron(s)
func (recur) Cron(schedule cron.Schedule) ScheduleOpt {
	return func(s *Schedule) error {
		if s.nexter != nil {
			return scheduleErr("cron", "multiple recurring options")
		}
		s.nexter = NexterFn(func(now utc.UTC, _ *Schedule) utc.UTC {
			return utc.New(schedule.Next(now.Time))
		})
		return nil
	}
}

type until struct{}

// Count limits the number of times the schedule is fired
func (until) Count(count int) ScheduleOpt {
	return func(s *Schedule) error {
		if s.maxCount > 0 {
			return scheduleErr("count", "multiple until count options")
		}
		s.maxCount = count
		return nil
	}
}

// Date specifies a maximum date for the schedule.
// If both Date and Elapsed are specified, the earliest resulting date is used.
func (until) Date(u utc.UTC) ScheduleOpt {
	return func(s *Schedule) error {
		if !s.limit.IsZero() {
			return scheduleErr("date", "multiple until date options")
		}
		s.limit = u
		return nil
	}
}

// Elapsed is the maximum duration of the schedule after first dispatching.
// If both Date and Elapsed are specified, the earliest resulting date is used.
func (until) Elapsed(d time.Duration) ScheduleOpt {
	return func(s *Schedule) error {
		if s.maxDuration != 0 {
			return scheduleErr("elapsed", "multiple until elapsed options")
		}
		s.maxDuration = d
		return nil
	}
}
