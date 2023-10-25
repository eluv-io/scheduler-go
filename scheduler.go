package scheduler

import (
	"log"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/eluv-io/errors-go"
	elog "github.com/eluv-io/log-go"
	"github.com/eluv-io/utc-go"
)

type Logger = *elog.Log

// Options are options for the Scheduler
type Options struct {
	ChannelSize   uint
	Logger        Logger
	OnChannelFull OnChannelFull
}

// OnChannelFull options tell the scheduler what to do when the dispatching channel is full
type OnChannelFull struct {
	SkipDispatch bool          // whether to skip dispatching a schedule if channel is full
	Block        bool          // send blocking if SkipDispatch is false and Block is true
	RetryAfter   time.Duration // if SkipDispatch is false and Block is false, duration to wait before retry
	MaxRetries   int           // max retries to dispatch
}

// NewOptions returns default options for the Scheduler.
func NewOptions() *Options {
	return &Options{
		ChannelSize: 1,
		OnChannelFull: OnChannelFull{
			SkipDispatch: false,
			RetryAfter:   time.Millisecond * 10,
			MaxRetries:   1,
		},
	}
}

// Scheduler represents a sequence of planned events which are notified through channel C().
type Scheduler struct {
	options   *Options
	stop      chan struct{}
	add       chan *Schedule
	remove    chan ScheduleID
	snapshot  chan chan []*Schedule
	running   bool
	runningMu sync.Mutex

	ch     chan Schedule
	logger Logger
}

// New returns a new Scheduler initialized with default options.
func New() *Scheduler {
	return NewScheduler(nil)
}

// NewScheduler returns a new Scheduler initialized with the given options.
// Default options are used if opts is nil.
func NewScheduler(opts *Options) *Scheduler {
	if opts == nil {
		opts = NewOptions()
	}
	if opts.OnChannelFull.RetryAfter < time.Millisecond {
		opts.OnChannelFull.RetryAfter = time.Millisecond
	}
	if opts.Logger == nil {
		log.Default()
		opts.Logger = elog.Get("/scheduler")
	}
	return &Scheduler{
		options:  opts,
		stop:     make(chan struct{}),
		add:      make(chan *Schedule),
		remove:   make(chan ScheduleID),
		snapshot: make(chan chan []*Schedule),
		ch:       make(chan Schedule, opts.ChannelSize),
		logger:   opts.Logger,
	}
}

// At adds a schedule that will run at the given date.
// Returns false if the scheduler is not running.
func (s *Scheduler) At(at utc.UTC, o interface{}) ScheduleID {
	id := ScheduleID(uuid.NewString())
	s.Add(&Schedule{
		id:     id,
		next:   at,
		object: o,
	})
	return id
}

// In adds a schedule that will run after the given duration.
// Returns false if the scheduler is not running.
func (s *Scheduler) In(d time.Duration, o interface{}) ScheduleID {
	id := ScheduleID(uuid.NewString())
	s.Add(&Schedule{
		id:      id,
		starter: NexterFn(func(now utc.UTC, _ *Schedule) utc.UTC { return now.Add(d) }),
		object:  o,
	})
	return id
}

// Add adds the given schedule to the scheduler.
// Returns false if the scheduler is not running.
func (s *Scheduler) Add(sc *Schedule) bool {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	if !s.running {
		return false
	}
	s.add <- sc
	return true
}

// Remove removes schedules(s) with the given ScheduleID
// Returns false if the scheduler is not running.
func (s *Scheduler) Remove(id ScheduleID) bool {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	if !s.running {
		return false
	}
	s.remove <- id
	return true
}

func (s *Scheduler) dump() []*Schedule {
	replyChan := make(chan []*Schedule, 1)
	s.snapshot <- replyChan
	return <-replyChan
}

// Dump returns a dump of the current schedules
func (s *Scheduler) Dump() []*Schedule {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	if !s.running {
		return []*Schedule{}
	}
	return s.dump()
}

// C provides a channel of notifications for planned events
func (s *Scheduler) C() chan Schedule {
	return s.ch
}

// Stop stops the scheduler
func (s *Scheduler) Stop() error {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	if !s.running {
		return errors.E("Stop", errors.K.Invalid, "reason", "not running")
	}
	_ = s.dumpStop(false)
	return nil
}

// DumpStop stops the scheduler and returns a dump of current schedules
func (s *Scheduler) DumpStop() ([]*Schedule, error) {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	if !s.running {
		return nil, errors.E("Stop", errors.K.Invalid, "reason", "not running")
	}
	ret := s.dumpStop(true)
	return ret, nil
}

func (s *Scheduler) dumpStop(dump bool) []*Schedule {
	var ret []*Schedule
	if dump {
		ret = s.dump()
	}
	s.stop <- struct{}{}
	s.running = false
	return ret
}

// Start starts the scheduler
func (s *Scheduler) Start() error {
	return s.run(nil)
}

// Run starts the scheduler with the given initial Schedule instances
func (s *Scheduler) Run(schedules []*Schedule) error {
	return s.run(schedules)
}

func (s *Scheduler) now() utc.UTC {
	return utc.Now()
}

func (s *Scheduler) run(schedules Schedules) error {
	const longAfter = 100000 * time.Hour

	s.runningMu.Lock()
	if s.running {
		s.runningMu.Unlock()
		return errors.E("run", errors.K.Invalid, "reason", "already running")
	}
	s.running = true
	s.runningMu.Unlock()

	go func() {
		s.logger.Info("start", "schedules", len(schedules))
		timer := time.NewTimer(longAfter)
		stopTimer := func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

		// make sure schedules have a next time initialised
		now := s.now()
		for _, entry := range schedules {
			entry.start(now)
		}

		for {
			stopTimer()

			// determine the next entry to run.
			now = s.now()
			sort.Sort(schedules)

			if len(schedules) == 0 {
				// no entries yet, just sleep
				timer.Reset(longAfter)
			} else {
				timer.Reset(schedules[0].next.Sub(now))
			}

			for {
				select {
				case tc := <-timer.C:
					now = utc.New(tc)
					s.logger.Trace("wake", "now", now)

					// dispatch entries whose next time is less or equal than now
					for _, entry := range schedules {
						if entry.next.After(now) {
							break
						}
						// update schedule & allow calling RescheduleAt
						entry.dispatching(s.now(), s)

						dispatched := false
						if !s.options.OnChannelFull.SkipDispatch && s.options.OnChannelFull.Block {
							// dispatch blocking
							s.ch <- entry.dispatchValue()
							entry.dispatched()
							dispatched = true
						} else {
							// dispatch non blocking
							retries := 0
							for {
								select {
								case s.ch <- entry.dispatchValue():
									entry.dispatched()
									dispatched = true
								default:
									// channel full
									switch s.options.OnChannelFull.SkipDispatch {
									case true:
										// just ignore and go ahead
									case false:
										if retries < s.options.OnChannelFull.MaxRetries {
											time.Sleep(s.options.OnChannelFull.RetryAfter)
											retries++
											continue
										}
									}
								}
								break
							}
						}
						schedules = schedules[1:]
						if entry.nextTime(now) {
							schedules = append(schedules, entry)
							s.logger.Trace("run", "schedule_at", now, "entry", entry.ID(), "dispatched", dispatched, "next", entry.next)
						} else {
							s.logger.Trace("run", "schedule_at", now, "entry", entry.ID(), "dispatched", dispatched)
						}
					}

				case entry := <-s.add:
					now = s.now()
					entry.start(now)
					schedules = append(schedules, entry)
					s.logger.Trace("added", "now", now, "entry", entry.ID(), "next", entry.next)

				case replyChan := <-s.snapshot:
					entries := make([]*Schedule, 0, len(schedules))
					for _, e := range schedules {
						entries = append(entries, e.copy())
					}
					replyChan <- entries
					continue

				case <-s.stop:
					stopTimer()

					close(s.ch)
					s.logger.Info("stop")
					return

				case id := <-s.remove:
					entries := Schedules(nil)
					for _, e := range schedules {
						if e.id != id {
							entries = append(entries, e)
						}
					}
					schedules = entries
					s.logger.Trace("removed", "entry", id)
				}

				break
			}
		}
	}()

	return nil
}
