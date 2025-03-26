package scheduler

import (
	"sync"

	"github.com/eluv-io/errors-go"
	elog "github.com/eluv-io/log-go"
	"github.com/eluv-io/utc-go"
)

// SchedExec is an example executor for a Scheduler and is used from the 'sched'
// command line.
type SchedExec struct {
	async    bool
	parallel int
	runFn    func(s *Schedule) bool
	stopFn   func(start utc.UTC, count int, s *Schedule) bool
	logger   *elog.Log
}

// NewSchedExec returns an initialized SchedExec. All parameters are mandatory.
// async:    when true execution is performed in a separate go-routine
// parallel: the maximum count of go-routines executing when async is true, 0 is for no limit
// runFn:    the function invoked when a Schedule is dispatched. Returns false to stop the scheduler
// stopFn:   returns true if the scheduler must be stopped. start is the date-time of the first
// dispatched schedule,count is the count of schedules already dispatched and s is the last schedule.
func NewSchedExec(
	async bool,
	parallel int,
	runFn func(s *Schedule) bool,
	stopFn func(start utc.UTC, count int, s *Schedule) bool,
	logger *elog.Log) (*SchedExec, error) {

	e := errors.Template("newExec", errors.K.Invalid)

	if stopFn == nil {
		return nil, e("reason", "no stop function")
	}
	if logger == nil {
		return nil, e("reason", "no logger")
	}
	if runFn == nil {
		return nil, e("reason", "no runFn function")
	}

	return &SchedExec{
		async:    async,
		parallel: parallel,
		stopFn:   stopFn,
		runFn:    runFn,
		logger:   logger,
	}, nil
}

func (o *SchedExec) RunWait(sc *Scheduler) {

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		o.Run(sc)
	}()

	wg.Wait()
}

func (o *SchedExec) Run(sched *Scheduler) {

	var limiter chan interface{}
	if o.async && o.parallel > 0 {
		limiter = make(chan interface{}, o.parallel)
	}

	var execWg *sync.WaitGroup
	if o.async {
		execWg = &sync.WaitGroup{}
	}

	defer func() {
		if limiter != nil {
			o.logger.Debug("waiting all workers return their permit")
			for i := 0; i < cap(limiter); i++ {
				limiter <- struct{}{}
			}
		}
		if execWg != nil {
			o.logger.Debug("waiting all workers done")
			execWg.Wait()
		}
	}()

	var start utc.UTC
	count := 0

out:
	for {
		select {
		case entry, ok := <-sched.C():
			if !ok {
				return
			}
			s := entry.S()
			count++
			if start == utc.Zero {
				start = s.Details().DispatchedAt
			}
			//fmt.Println("id", s.ID(),
			//	"scheduled at", s.Details().ScheduledAt,
			//	"dispatched at", s.Details().DispatchedAt,
			//	"next", s.Details().Next)

			switch {
			case o.async:
				switch limiter {
				case nil:
				default:
					select {
					case limiter <- struct{}{}:
					default:
						o.logger.Info("max parallel reached - skipping schedule",
							"id", s.ID(),
							"dispatched at", s.Details().DispatchedAt)
						continue
					}
				}

				execWg.Add(1)
				go func(s *Schedule) {
					defer func() {
						if limiter != nil {
							<-limiter
						}
						execWg.Done()
						if o.stopFn(start, 0, s) { // count was already evaluated
							_ = sched.Stop()
						}
					}()
					if !o.runFn(s) {
						_ = sched.Stop()
					}
				}(s)

			default:
				if !o.runFn(s) {
					_ = sched.Stop()
					break out
				}
			}

			// stop the scheduler if the entry is not repeatable
			if o.stopFn(start, count, s) {
				_ = sched.Stop()
			}
		}
	}

}
