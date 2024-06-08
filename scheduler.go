package scheduler

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/eluv-io/errors-go"
	elog "github.com/eluv-io/log-go"
	"github.com/eluv-io/utc-go"
)

const (
	defaultMaxWait = time.Millisecond * 20
)

type Logger = *elog.Log

// Options are options for the Scheduler
type Options struct {
	ChannelSize   uint          // size of the notification channel
	Logger        Logger        // internal logger
	OnChannelFull OnChannelFull // what to do when the channel is full
	ClockHealth   ClockHealth   // check the system clock is in sync with the monotonic clock
}

// OnChannelFull options tell the scheduler what to do when the dispatching channel is full
type OnChannelFull struct {
	MaxWait time.Duration // max duration to wait for dispatching a schedule
}

// NewOptions returns default options for the Scheduler.
func NewOptions() *Options {
	return &Options{
		ChannelSize: 1,
		OnChannelFull: OnChannelFull{
			MaxWait: defaultMaxWait,
		},
	}
}

// Scheduler represents a sequence of planned events which are notified through channel C().
type Scheduler struct {
	options   *Options
	reset     chan struct{}
	stop      chan struct{}
	add       chan *Schedule
	remove    chan ScheduleID
	snapshot  chan chan []*Schedule
	running   bool
	runningMu sync.Mutex
	outlet    []*Schedule
	outletMu  sync.Mutex

	ch     chan Entry
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
	if opts.OnChannelFull.MaxWait <= 0 {
		opts.OnChannelFull.MaxWait = defaultMaxWait
	}
	if opts.Logger == nil {
		log.Default()
		opts.Logger = elog.Get("/scheduler")
	}
	return &Scheduler{
		options:  opts,
		reset:    make(chan struct{}),
		stop:     make(chan struct{}, 1),
		add:      make(chan *Schedule),
		remove:   make(chan ScheduleID),
		snapshot: make(chan chan []*Schedule),
		ch:       make(chan Entry, opts.ChannelSize),
		logger:   opts.Logger,
	}
}

// At adds a schedule that will run at the given date.
// Returns false if the scheduler is not running.
func (s *Scheduler) At(at utc.UTC, o interface{}) ScheduleID {
	id := ScheduleID(uuid.NewString())
	s.Add(&Schedule{
		id:     id,
		state:  &atomic.Int64{},
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
		state:   &atomic.Int64{},
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

func (s *Scheduler) Running() bool {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	return s.running
}

// C provides a channel of notifications for planned events
func (s *Scheduler) C() chan Entry {
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

// Outlet returns the schedules that could not be dispatched because the channel was full
func (s *Scheduler) Outlet() []*Schedule {
	s.outletMu.Lock()
	defer s.outletMu.Unlock()
	ret := s.outlet
	s.outlet = nil
	return ret
}

func (s *Scheduler) overflowed(entry *Schedule) {
	s.outletMu.Lock()
	defer s.outletMu.Unlock()
	entry.dispatchTimedOut()
	s.outlet = append(s.outlet, entry)
}

func (s *Scheduler) now() utc.UTC {
	return utc.Now().Round(0) // use the wall clock
}

func (s *Scheduler) run(schedules Schedules) error {
	const inLongTime = 100000 * time.Hour

	s.runningMu.Lock()
	if s.running {
		s.runningMu.Unlock()
		return errors.E("run", errors.K.Invalid, "reason", "already running")
	}
	s.running = true
	s.runningMu.Unlock()

	dumpSchedules := func(replyChan chan []*Schedule) {
		entries := make([]*Schedule, 0, len(schedules))
		for _, e := range schedules {
			entries = append(entries, e.copy())
		}
		sort.Sort(Schedules(entries))
		replyChan <- entries
	}

	addEntry := func(entry *Schedule) {
		now := s.now()
		entry.start(now)
		schedules = append(schedules, entry)
		s.logger.Trace("added", "now", now, "id", entry.ID(), "next", entry.next)
	}

	remEntry := func(id ScheduleID) {
		entries := Schedules(nil)
		for _, e := range schedules {
			if e.id != id {
				entries = append(entries, e)
			}
		}
		schedules = entries
		s.logger.Trace("removed", "entry", id)
	}

	var clockHealth *clockHealth
	if s.options.ClockHealth.Enabled {
		clockHealth = newClockHealth(s, s.options.ClockHealth)
		clockHealth.run()
	}

	go func() {
		s.logger.Info("start", "schedules", len(schedules))
		timer := time.NewTimer(inLongTime)
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

			in := inLongTime // just sleep when no entries yet
			id := "-"
			if len(schedules) > 0 {
				in = schedules[0].next.Sub(now)
				id = string(schedules[0].id)
			}
			s.logger.Trace("next schedule", "id", id,
				"now", now,
				"in", in,
				"next", now.Add(in))
			timer.Reset(in)

			for {
				select {
				case <-s.reset:
					// do nothing, just reset timer
				case tc := <-timer.C:
					now = utc.New(tc).Round(0)
					s.logger.Trace("wake", "now", now)
					breakSchedules := false

					// dispatch entries whose next time is less or equal than now
					// since dispatch is blocking, other events need to be taken
					// into account (can potentially block for a long time).
					for _, entry := range schedules {
						if entry.next.After(now) || breakSchedules {
							break
						}
						dispatched := false
						switch entry.getState() {
						case scDispatched:
							// entry is still in 'dispatched' state because it was not delivered
							// which may happen for recurrent entries that were dispatched and
							// rescheduled but the executor side did not handle the entry yet due
							// to being busy with some other earlier entry.
							dispatched = true
							s.logger.Warn("run: skipping dispatched entry", "schedule_at", now, "id", entry.ID())
						default:
							// update schedule & allow calling RescheduleAt
							entry.dispatching(s.now(), s)

							dispatchTimer := time.NewTimer(s.options.OnChannelFull.MaxWait)
							s.logger.Trace("dispatching", "id", entry.ID())
							for {
								select {
								case s.ch <- entry.dispatchValue():
									entry.dispatched()
									dispatched = true
									if !dispatchTimer.Stop() {
										<-dispatchTimer.C
									}
								case entry := <-s.add:
									addEntry(entry)
									breakSchedules = true
									continue
								case id := <-s.remove:
									if id != entry.id {
										remEntry(id)
										breakSchedules = true
										continue
									}
								case replyChan := <-s.snapshot:
									dumpSchedules(replyChan)
									continue
								case <-s.stop:
									// send back to stop
									s.stop <- struct{}{}
									if !dispatchTimer.Stop() {
										<-dispatchTimer.C
									}
									breakSchedules = true
								case <-dispatchTimer.C:
									// give up
								}
								break
							}
						}

						if !dispatched {
							s.overflowed(entry)
						}

						if !dispatched {
							s.overflowed(entry)
						}

						schedules = schedules[1:]
						if entry.nextTime(now) {
							schedules = append(schedules, entry)
							s.logger.Trace("run - reschedule", "schedule_at", now, "id", entry.ID(), "dispatched", dispatched, "next", entry.next)
						} else {
							if dispatched {
								s.logger.Trace("run - done", "schedule_at", now, "id", entry.ID(), "dispatched", dispatched)
							} else {
								s.logger.Warn("run - dispatch failed", "schedule_at", now, "id", entry.ID(), "dispatched", dispatched)
							}
						}
					}

				case entry := <-s.add:
					addEntry(entry)

				case id := <-s.remove:
					remEntry(id)

				case replyChan := <-s.snapshot:
					dumpSchedules(replyChan)
					continue

				case <-s.stop:
					stopTimer()

					s.runningMu.Lock()
					s.running = false
					s.runningMu.Unlock()

					close(s.ch)
					if clockHealth != nil {
						clockHealth.halt()
					}
					s.logger.Info("stop")
					return
				}

				break
			}
		}
	}()

	return nil
}
