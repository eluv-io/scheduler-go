package scheduler

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/eluv-io/errors-go"
	"github.com/eluv-io/utc-go"
)

// ScheduleID is the ID of a Schedule
type ScheduleID string

type Entry interface {
	S() *Schedule
}

// NewSchedule returns a new initialized Schedule or an error if incompatible options are used.
// The provided options must set the next scheduled time (e.g. through an Occur or a Recur option)
func NewSchedule(id string, o interface{}, opts ...ScheduleOpt) (*Schedule, error) {
	ret := &Schedule{
		id:     ScheduleID(id),
		state:  &atomic.Int64{},
		object: o,
	}
	ret.state.Store(int64(scInit))
	var err error
	for _, opt := range opts {
		err = errors.Append(err, opt(ret))
	}
	if ret.next.IsZero() && ret.starter == nil && ret.nexter == nil {
		err = errors.Append(err, errors.NoTrace("NewSchedule", errors.K.Invalid,
			"reason", "no time scheduled"))
	}
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// MustSchedule returns a new initialized Schedule or panics if an error occurs.
func MustSchedule(id string, o interface{}, opts ...ScheduleOpt) *Schedule {
	ret, err := NewSchedule(id, o, opts...)
	if err != nil {
		panic(err)
	}
	return ret
}

type scheduleState int64

const (
	scInit scheduleState = iota
	scScheduled
	scDispatching
	scDispatched
	scDelivered
	scDispatchTimedOut
	scDone
)

// Schedule is a planned event.
type Schedule struct {
	id          ScheduleID    // ID of the schedule
	next        utc.UTC       // next time this Schedule must be notified
	starter     Nexter        // computing 'next' for the first time
	object      interface{}   // the associated 'thing' that is scheduled
	nexter      Nexter        // computing 'next' time (for repeating or cron schedules)
	maxCount    int           // number of times to dispatch this Schedule (or zero if no limit)
	limit       utc.UTC       // limit date or zero if no limit
	maxDuration time.Duration // limit duration computed on first dispatching
	scheduler   *Scheduler    // the scheduler for rescheduling or nil
	state       *atomic.Int64 // state of the schedule
	details     Details       // details of the Schedule
	muDetails   sync.Mutex    // protect public details
	pubDetails  Details       // details made accessible
}

// Details of Schedule are available for logging or troubleshooting
type Details struct {
	ScheduledAt      utc.UTC `json:"scheduled_at"`                // time the Schedule was scheduled
	DispatchedAt     utc.UTC `json:"dispatched_at"`               // when the schedule was sent to the notification channel
	DispatchedCount  int     `json:"dispatched_count,omitempty"`  // how many times the schedule was dispatched
	RescheduledCount int     `json:"rescheduled_count,omitempty"` // how many times the schedule was re-scheduled
}

func (s *Schedule) S() *Schedule {
	s.setState(scDelivered)
	return s
}

func (s *Schedule) getState() scheduleState {
	return scheduleState(s.state.Load())
}

func (s *Schedule) setState(state scheduleState) {
	s.state.Store(int64(state))
}

func (s *Schedule) start(now utc.UTC) {
	if s.next.IsZero() && s.starter != nil {
		s.next = s.starter.Next(now, s)
	}
	if s.next.IsZero() && s.nexter != nil {
		s.next = s.nexter.Next(now, s)
	}
	s.updateDetails()
}

// nextTime returns true if the Schedule must be rescheduled by the scheduler.
func (s *Schedule) nextTime(now utc.UTC) bool {
	if s.nexter == nil {
		s.next = utc.Zero
		return false
	}
	if s.maxCount > 0 && s.details.DispatchedCount == s.maxCount {
		s.next = utc.Zero
		return false
	}
	next := s.nexter.Next(now, s)
	if next.IsZero() {
		s.next = utc.Zero
		return false
	}
	if !s.limit.IsZero() && next.After(s.limit) {
		s.next = utc.Zero
		return false
	}
	s.next = next
	return true
}

func (s *Schedule) dispatching(now utc.UTC, sc *Scheduler) {
	s.details.ScheduledAt = s.next
	if s.details.DispatchedAt.IsZero() && s.maxDuration > 0 {
		l := now.Add(s.maxDuration)
		if !s.limit.IsZero() && s.limit.Before(l) {
			l = s.limit
		}
		s.limit = l
	}
	s.details.DispatchedAt = now
	s.scheduler = sc
	s.setState(scDispatching)
}

func (s *Schedule) dispatchValue() *Schedule {
	// first version of implementation was dispatching Schedule (not *Schedule)
	//ret := *s
	//ret.details.DispatchedCount++
	return s
}

func (s *Schedule) dispatched() {
	// use CAS because the other side may have already updated the schedule to 'delivered'
	s.state.CompareAndSwap(int64(scDispatching), int64(scDispatched))
	s.details.DispatchedCount++
}

func (s *Schedule) dispatchTimedOut() {
	s.setState(scDispatchTimedOut)
}

func (s *Schedule) done() {
	s.setState(scDone)
}

func (s *Schedule) String() string {
	if s.next == utc.Zero {
		return fmt.Sprintf("schedule[id: %s, at: %v, dat: %v]", s.id,
			s.details.ScheduledAt,
			s.details.DispatchedAt)
	}
	return fmt.Sprintf("schedule[id: %s, at: %v, dat: %v, next: %v]", s.id,
		s.details.ScheduledAt,
		s.details.DispatchedAt,
		s.next)
}

func (s *Schedule) MarshalJSON() ([]byte, error) {
	type sched struct {
		Id   string  `json:"id"`
		At   utc.UTC `json:"at"`
		Dat  utc.UTC `json:"dat"`
		Next utc.UTC `json:"next"`
	}
	js := &sched{
		Id:   string(s.id),
		At:   s.details.ScheduledAt,
		Dat:  s.details.DispatchedAt,
		Next: s.next,
	}
	return json.Marshal(js)
}

func (s *Schedule) same(o *Schedule) bool {
	return s.id == o.id &&
		s.next == o.next &&
		s.object == o.object &&
		s.maxCount == o.maxCount &&
		s.limit == o.limit &&
		s.maxDuration == o.maxDuration &&
		s.details == o.details
}

func (s *Schedule) copy() *Schedule {
	state := &atomic.Int64{}
	state.Store(int64(s.getState()))
	return &Schedule{
		id:          s.id,
		state:       state,
		next:        s.next,
		object:      s.object,
		maxCount:    s.maxCount,
		limit:       s.limit,
		maxDuration: s.maxDuration,
		details:     s.details,
	}
}

// ID returns the id of the schedule
func (s *Schedule) ID() ScheduleID {
	return s.id
}

// Time returns the utc time at which the schedule was planned
func (s *Schedule) Time() utc.UTC {
	return s.details.ScheduledAt
}

// Object returns the interface object attached to the Schedule
func (s *Schedule) Object() interface{} {
	return s.object
}

// Details returns the 'runtime' Details of the schedule
func (s *Schedule) Details() Details {
	s.muDetails.Lock()
	defer s.muDetails.Unlock()
	return s.pubDetails
}

func (s *Schedule) updateDetails() {
	s.muDetails.Lock()
	defer s.muDetails.Unlock()
	s.pubDetails = s.details
}

// Str returns the object attached to this Schedule as a string or the empty
// string if the object is not a string.
func (s *Schedule) Str() string {
	if str, ok := s.object.(string); ok {
		return str
	}
	return ""
}

// Fn returns the object attached to this Schedule as a func() or nil if not such a function.
func (s *Schedule) Fn() func() {
	if fn, ok := s.object.(func()); ok {
		return fn
	}
	return nil
}

// FnE returns the object attached to this Schedule as a func() error or nil if not such a function.
func (s *Schedule) FnE() func() error {
	if fn, ok := s.object.(func() error); ok {
		return fn
	}
	return nil
}

func (s *Schedule) canReschedule(at string) bool {
	if s.scheduler == nil {
		return false
	}
	if s.nexter != nil {
		s.scheduler.logger.Warn(fmt.Sprintf("'%s': already rescheduled by 'nexter'", at),
			"entry", s.ID(), "next", s.next)
		return false
	}
	if s.getState() == scScheduled {
		// prevent multiple scheduling
		return false
	}
	return true
}

// RescheduleAt reschedules the schedule at the given date. The function returns
// false if the schedule was built via a Recur option or if the schedule was not
// dispatched by the scheduler. Returns true if the schedule was sent to the scheduler.
func (s *Schedule) RescheduleAt(t utc.UTC, o ...interface{}) bool {
	if !s.canReschedule("at") {
		return false
	}
	s.details.RescheduledCount++
	s.next = t
	if len(o) > 0 {
		s.object = o[0]
	}
	return s.scheduler.Add(s)
}

// RescheduleIn reschedules the schedule at a date in d after now. The date is
// computed immediately.
// The function returns false if the schedule was built via a Recur option or if
// the schedule was not dispatched by the scheduler. Returns true if the schedule
// was sent to the scheduler.
func (s *Schedule) RescheduleIn(d time.Duration, o ...interface{}) bool {
	if !s.canReschedule("in") {
		return false
	}
	s.details.RescheduledCount++
	s.next = s.scheduler.now().Add(d)
	if len(o) > 0 {
		s.object = o[0]
	}
	return s.scheduler.Add(s)
}

var (
	_ sort.Interface = (Schedules)(nil)
)

// Schedules is a slice of *Schedule sortable by their next utc time.
type Schedules []*Schedule

func (s Schedules) Len() int {
	return len(s)
}

func (s Schedules) Less(i, j int) bool {
	return s[i].next.Before(s[j].next)
}

func (s Schedules) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Schedules) copyNoDup() (Schedules, error) {
	ret := make(Schedules, 0, len(s))
	ex := make(map[uintptr]*Schedule)
	for _, sc := range s {
		if sc == nil {
			return nil, errors.E("copyNoDup", errors.K.Invalid,
				"reason", "nil schedule")
		}
		ptr := uintptr(unsafe.Pointer(sc))
		if ex[ptr] != nil {
			return nil, errors.E("copyNoDup", errors.K.Invalid,
				"reason", "duplicate schedule",
				"schedule", sc.id)
		}
		ex[ptr] = sc
		ret = append(ret, sc)
	}
	return ret, nil
}
