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

const (
	defaultMaxWait = time.Millisecond * 20
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
	if opts.OnChannelFull.MaxWait <= 0 {
		opts.OnChannelFull.MaxWait = defaultMaxWait
	}
	if opts.Logger == nil {
		log.Default()
		opts.Logger = elog.Get("/scheduler")
	}
	return &Scheduler{
		options:  opts,
		stop:     make(chan struct{}, 1),
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

func (s *Scheduler) Running() bool {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	return s.running
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
		s.logger.Trace("added", "now", now, "entry", entry.ID(), "next", entry.next)
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

			if len(schedules) == 0 {
				// no entries yet, just sleep
				timer.Reset(inLongTime)
			} else {
				timer.Reset(schedules[0].next.Sub(now))
			}

			for {
				select {
				case tc := <-timer.C:
					now = utc.New(tc)
					s.logger.Trace("wake", "now", now)
					breakSchedules := false

					// dispatch entries whose next time is less or equal than now
					// since dispatch is blocking, other events need to be taken
					// into account (can potentially block for a long time).
					for _, entry := range schedules {
						if entry.next.After(now) || breakSchedules {
							break
						}
						// update schedule & allow calling RescheduleAt
						entry.dispatching(s.now(), s)
						dispatched := false

						dispatchTimer := time.NewTimer(s.options.OnChannelFull.MaxWait)
						s.logger.Trace("dispatching", entry.ID())
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

						schedules = schedules[1:]
						if entry.nextTime(now) {
							schedules = append(schedules, entry)
							s.logger.Trace("run", "schedule_at", now, "entry", entry.ID(), "dispatched", dispatched, "next", entry.next)
						} else {
							s.logger.Trace("run", "schedule_at", now, "entry", entry.ID(), "dispatched", dispatched)
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
					s.logger.Info("stop")
					return
				}

				break
			}
		}
	}()

	return nil
}
