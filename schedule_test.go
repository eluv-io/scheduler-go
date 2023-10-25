package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/eluv-io/utc-go"
)

func TestScheduleStart(t *testing.T) {
	type testCase struct {
		descr string
		opts  []ScheduleOpt
		fn    func(s *Schedule)
		want  *Schedule
	}
	u := utc.Now()

	for _, tc := range []*testCase{
		{
			descr: "at",
			opts:  []ScheduleOpt{Occur.At(u)},
			want: &Schedule{
				id:   "at",
				next: u,
			},
		},
		{
			descr: "in",
			opts:  []ScheduleOpt{Occur.In(time.Hour)},
			fn:    func(s *Schedule) { s.start(u) },
			want: &Schedule{
				id:   "in",
				next: u.Add(time.Hour),
			},
		},
		{
			descr: "every",
			opts:  []ScheduleOpt{Recur.Every(time.Hour)},
			fn:    func(s *Schedule) { s.start(u) },
			want: &Schedule{
				id:   "every",
				next: u.Add(time.Hour),
			},
		},
	} {
		s, err := NewSchedule(tc.descr, nil, tc.opts...)
		require.NoError(t, err, tc.descr)
		if tc.fn != nil {
			tc.fn(s)
		}
		require.True(t, tc.want.same(s), tc.descr)
	}
}

// dispatch mimics dispatching by scheduler
func dispatch(now utc.UTC, entry *Schedule) bool {
	entry.dispatching(now, nil)
	entry.dispatched()
	return entry.nextTime(now)
}

func TestScheduleEvery(t *testing.T) {
	u := utc.Now()
	s, err := NewSchedule("1", nil,
		Recur.Every(time.Hour),
	)
	require.NoError(t, err)
	s.start(u) // no start date

	u = u.Add(time.Hour)
	require.Equal(t, u, s.next)
	require.True(t, dispatch(u, s))
	require.Equal(t, u.Add(time.Hour), s.next)
}

func TestScheduleOccurThenEvery(t *testing.T) {
	u := utc.Now()
	s, err := NewSchedule("1", nil,
		Occur.At(u),
		Recur.Every(time.Hour),
	)
	require.NoError(t, err)

	require.Equal(t, u, s.next)
	require.True(t, dispatch(u, s))
	require.Equal(t, u.Add(time.Hour), s.next)
}

func TestScheduleOccurThenEveryUntilCount(t *testing.T) {
	u := utc.Now()
	s, err := NewSchedule("1", nil,
		Occur.At(u),
		Recur.Every(time.Hour),
		Until.Count(2),
	)
	require.NoError(t, err)

	require.Equal(t, u, s.next)
	require.True(t, dispatch(u, s))
	u = u.Add(time.Hour)
	require.Equal(t, u, s.next)

	require.False(t, dispatch(u, s))
	require.Equal(t, 2, s.details.DispatchedCount)
}

func TestScheduleOccurThenEveryUntilDate(t *testing.T) {
	u := utc.Now()
	s, err := NewSchedule("1", nil,
		Occur.At(u),
		Recur.Every(time.Hour),
		Until.Date(u.Add(time.Hour*2)),
	)
	require.NoError(t, err)

	require.Equal(t, u, s.next)
	require.True(t, dispatch(u, s))
	u = u.Add(time.Hour)
	require.Equal(t, u, s.next)

	require.True(t, dispatch(u, s))
	u = u.Add(time.Hour)
	require.Equal(t, u, s.next)
	require.Equal(t, 2, s.details.DispatchedCount)

	require.False(t, dispatch(u, s))
}

func TestScheduleErrors(t *testing.T) {
	type testCase struct {
		descr string
		opts  []ScheduleOpt
	}
	u := utc.Now()

	for _, tc := range []*testCase{
		{
			descr: "no time",
		},
		{
			descr: "multiple occur at",
			opts:  []ScheduleOpt{Occur.At(u), Occur.At(u.Add(time.Hour))},
		},
		{
			descr: "multiple occur in",
			opts:  []ScheduleOpt{Occur.In(time.Minute), Occur.In(time.Hour)},
		},
		{
			descr: "multiple occur at/in",
			opts:  []ScheduleOpt{Occur.At(u), Occur.In(time.Hour)},
		},
		{
			descr: "multiple recur",
			opts:  []ScheduleOpt{Occur.In(time.Minute), Recur.Every(time.Minute), Recur.Every(time.Hour)},
		},
		{
			descr: "multiple until date",
			opts:  []ScheduleOpt{Occur.In(time.Minute), Until.Date(u), Until.Date(u.Add(time.Hour))},
		},
		{
			descr: "multiple until elapsed",
			opts:  []ScheduleOpt{Occur.In(time.Minute), Until.Elapsed(time.Minute), Until.Elapsed(time.Hour)},
		},
		{
			descr: "multiple count",
			opts:  []ScheduleOpt{Occur.In(time.Minute), Until.Count(2), Until.Count(3)},
		},
	} {
		_, err := NewSchedule(tc.descr, nil, tc.opts...)
		require.Error(t, err, tc.descr)
	}
}
