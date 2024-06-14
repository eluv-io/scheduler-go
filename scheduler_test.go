package scheduler

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"

	"github.com/eluv-io/utc-go"
)

func TestOccur(t *testing.T) {
	sc := New()

	startIn := time.Millisecond * 200
	step := time.Millisecond * 10
	schedules := []*Schedule{
		MustSchedule("1", nil, Occur.In(startIn)),
		MustSchedule("3", nil, Occur.In(startIn+step*2)),
		MustSchedule("2", nil, Occur.In(startIn+step)),
	}

	receiver := NewSimpleReceiver(sc.C())
	receiver.run(func(*Schedule) {
		if len(receiver.received) == len(schedules) {
			scheds, err := sc.DumpStop()
			require.NoError(t, err)
			require.Equal(t, 0, len(scheds))
		}
	})

	err := sc.Run(schedules)
	require.NoError(t, err)
	receiver.wait()

	receiver.assertReceivedIds(t, []string{"1", "2", "3"})
	require.False(t, sc.Running())
}

func TestAdd(t *testing.T) {
	sc := New()

	receiver := NewSimpleReceiver(sc.C())
	receiver.run(func(*Schedule) {
		if len(receiver.received) == 3 {
			scheds, err := sc.DumpStop()
			require.NoError(t, err)
			require.Equal(t, 0, len(scheds))
		}
	})
	err := sc.Run([]*Schedule{
		MustSchedule("3", nil, Occur.In(time.Second)),
	})
	go func(sc *Scheduler) {
		sc.Add(MustSchedule("2", nil, Occur.In(time.Millisecond*700)))
		sc.Add(MustSchedule("1", nil, Occur.In(time.Millisecond*400)))
	}(sc)
	require.NoError(t, err)
	receiver.wait()

	receiver.assertReceivedIds(t, []string{"1", "2", "3"})
	require.False(t, sc.Running())
}

func TestRecur(t *testing.T) {
	sc := New()

	now := utc.Now().Add(time.Second + time.Millisecond*200).Truncate(time.Second)

	cronSched, err := cron.ParseStandard("@every 1s")
	require.NoError(t, err)

	schedules := []*Schedule{
		// put the cron schedule first, since cron will round to the second
		MustSchedule("1", nil,
			Occur.At(now),
			Recur.Cron(cronSched),
			Until.Count(3)),
		MustSchedule("3", nil,
			Occur.At(now.Add(time.Millisecond*2)),
			Recur.Every(time.Second),
			Until.Count(3)),
		MustSchedule("2", nil,
			Occur.At(now.Add(time.Millisecond)),
			Recur.NextTime(NexterTimeFn(func(now time.Time, s *Schedule) time.Time { return now.Add(time.Second) })),
			Until.Count(3)),
	}

	receiver := NewSimpleReceiver(sc.C())
	receiver.run(func(s *Schedule) {
		sc.logger.Info("<-", "id", s.ID())
		if len(receiver.received) == len(schedules)*3 {
			scheds, err := sc.DumpStop()
			require.NoError(t, err)
			require.Equal(t, 0, len(scheds))
		}
	})

	err = sc.Run(schedules)
	require.NoError(t, err)
	receiver.wait()

	receiver.assertReceivedIds(t, []string{
		"1", "2", "3",
		"1", "2", "3",
		"1", "2", "3",
	})
	require.False(t, sc.Running())
}

func TestCronEverySecondUntil(t *testing.T) {
	cp := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.DowOptional | cron.Descriptor)
	cronSched, err := cp.Parse("* * * * * *")
	require.NoError(t, err)

	sc := New()

	now := utc.Now().Add(time.Second + time.Millisecond*200).Truncate(time.Second)
	max := now.Add(time.Second * 2).Add(time.Millisecond)
	elapsed := time.Second*2 + time.Millisecond

	schedules := []*Schedule{
		MustSchedule("1", nil,
			Occur.At(now),
			Recur.Cron(cronSched),
			Until.Date(max)),
		MustSchedule("2", nil,
			Occur.At(now.Add(time.Second*3)),
			Recur.Cron(cronSched),
			Until.Elapsed(elapsed)),
	}

	receiver := NewSimpleReceiver(sc.C())
	receiver.run(func(s *Schedule) {
		sc.logger.Info("<-", "id", s.ID())
		if len(receiver.received) == 6 {
			scheds, err := sc.DumpStop()
			require.NoError(t, err)
			require.Equal(t, 0, len(scheds))
		}
	})

	err = sc.Run(schedules)
	require.NoError(t, err)
	receiver.wait()

	receiver.assertReceivedIds(t, []string{
		"1", "1", "1",
		"2", "2", "2",
	})
	require.False(t, sc.Running())
}

func TestDispatchChannelFull(t *testing.T) {
	for _, skipDispatch := range []bool{true, false} {

		t.Run(fmt.Sprintf("skipDispatch-%v", skipDispatch), func(t *testing.T) {
			maxWait := time.Millisecond * 10
			if skipDispatch {
				maxWait = time.Millisecond
			}
			sc := NewScheduler(&Options{
				ChannelSize: 0,
				OnChannelFull: OnChannelFull{
					MaxWait: maxWait,
				},
			})

			startIn := time.Millisecond * 200
			step := time.Millisecond * 10
			schedules := []*Schedule{
				MustSchedule("1", nil, Occur.In(startIn)),
				MustSchedule("3", nil, Occur.In(startIn+step*2)),
				MustSchedule("2", nil, Occur.In(startIn+step)),
			}

			receiver := NewSimpleReceiver(sc.C())
			receiver.run(func(s *Schedule) {
				if len(receiver.received) == 1 {
					// pause receiving after the first one received
					time.Sleep(time.Millisecond * 15)
				}
				if s.ID() == "3" {
					scheds, err := sc.DumpStop()
					require.NoError(t, err)
					require.Equal(t, 0, len(scheds))
				}
			})

			err := sc.Run(schedules)
			require.NoError(t, err)
			receiver.wait()

			fmt.Println(strings.Join(receiver.receivedString(), "\n"))
			if skipDispatch {
				receiver.assertReceivedIds(t, []string{"1", "3"})
			} else {
				receiver.assertReceivedIds(t, []string{"1", "2", "3"})
			}
			require.False(t, sc.Running())
		})
	}
}

func TestDispatchBlocking(t *testing.T) {

	sc := NewScheduler(&Options{
		ChannelSize: 0,
		OnChannelFull: OnChannelFull{
			MaxWait: defaultMaxWait,
		},
	})

	startIn := time.Millisecond * 200
	step := time.Millisecond * 10
	schedules := []*Schedule{
		MustSchedule("1", nil, Occur.In(startIn)),
		MustSchedule("3", nil, Occur.In(startIn+step*2)),
		MustSchedule("2", nil, Occur.In(startIn+step)),
	}

	receiver := NewSimpleReceiver(sc.C())
	receiver.run(func(s *Schedule) {
		time.Sleep(time.Millisecond)
		if s.ID() == "3" {
			scheds, err := sc.DumpStop()
			require.NoError(t, err)
			require.Equal(t, 0, len(scheds))
		}
	})

	err := sc.Run(schedules)
	require.NoError(t, err)
	receiver.wait()

	fmt.Println(strings.Join(receiver.receivedString(), "\n"))
	receiver.assertReceivedIds(t, []string{"1", "2", "3"})
	require.False(t, sc.Running())
}

func TestDispatchStopWhenFull(t *testing.T) {

	sc := NewScheduler(&Options{
		ChannelSize: 0,
		OnChannelFull: OnChannelFull{
			MaxWait: time.Minute,
		},
	})

	startIn := time.Millisecond * 200
	schedules := []*Schedule{
		MustSchedule("1", nil, Occur.In(startIn)),
	}

	w := utc.Now()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 400)
		scheds, err := sc.DumpStop()
		fmt.Println("stopped after", utc.Now().Sub(w))
		require.NoError(t, err)
		require.Equal(t, 1, len(scheds))
		require.Equal(t, "1", string(scheds[0].id))
	}()

	err := sc.Run(schedules)
	require.NoError(t, err)
	wg.Wait()
	<-sc.C()
	require.False(t, sc.Running())
}

func TestReschedule(t *testing.T) {
	sc := New()

	delay := time.Millisecond * 200
	schedules := []*Schedule{
		MustSchedule("1", nil, Occur.In(delay)),
	}

	receiver := NewSimpleReceiver(sc.C())
	receiver.run(func(s *Schedule) {
		if len(receiver.received) > 1 {
			require.NotEmpty(t, s.Str())
			c, err := strconv.Atoi(s.Str())
			require.NoError(t, err)
			require.Equal(t, len(receiver.received)-1, c)
		}
		if len(receiver.received) == 3 {
			scheds, err := sc.DumpStop()
			require.NoError(t, err)
			require.Equal(t, 0, len(scheds))
			return
		}
		ok := s.RescheduleAt(utc.Now().Add(delay), fmt.Sprintf("%d", len(receiver.received)))
		require.True(t, ok)
		ok = s.RescheduleAt(utc.Now().Add(delay))
		require.False(t, ok)
	})

	err := sc.Run(schedules)
	require.NoError(t, err)
	receiver.wait()

	receiver.assertReceivedIds(t, []string{"1", "1", "1"})
	fmt.Println(strings.Join(receiver.receivedString(), "\n"))
	require.InDelta(t,
		receiver.received[0].details.ScheduledAt.Add(delay).UnixMilli(),
		receiver.received[1].details.ScheduledAt.UnixMilli(),
		float64(time.Millisecond*2))
	require.InDelta(t,
		receiver.received[1].details.ScheduledAt.Add(delay).UnixMilli(),
		receiver.received[2].details.ScheduledAt.UnixMilli(),
		float64(time.Millisecond*2))
	require.False(t, sc.Running())
}

// TestAddRemove adds and removes schedules while the scheduler is running and
// not dispatching (schedules are in the future).
func TestAddRemove(t *testing.T) {
	sc := New()

	receiver := NewSimpleReceiver(sc.C())
	receiver.run(nil)

	err := sc.Start()
	require.NoError(t, err)

	startIn := time.Hour
	step := time.Millisecond * 10
	schedules := []*Schedule{
		MustSchedule("1", nil, Occur.In(startIn)),
		MustSchedule("3", nil, Occur.In(startIn+step*2)),
		MustSchedule("2", nil, Occur.In(startIn+step)),
	}
	{
		ok := sc.Add(schedules[0])
		require.True(t, ok)

		dump := sc.Dump()
		require.Equal(t, 1, len(dump))
		require.True(t, schedules[0].same(dump[0]))
	}
	{
		ok := sc.Add(schedules[1])
		require.True(t, ok)

		dump := sc.Dump()
		require.Equal(t, 2, len(dump))
		require.True(t, schedules[0].same(dump[0]))
		require.True(t, schedules[1].same(dump[1]))
	}
	{
		ok := sc.Add(schedules[2])
		require.True(t, ok)

		dump := sc.Dump()
		require.Equal(t, 3, len(dump))
		require.True(t, schedules[0].same(dump[0]))
		require.True(t, schedules[2].same(dump[1]))
		require.True(t, schedules[1].same(dump[2]))
	}

	ok := sc.Remove(schedules[2].ID())
	require.True(t, ok)

	dump := sc.Dump()
	require.Equal(t, 2, len(dump))
	require.True(t, schedules[0].same(dump[0]))
	require.True(t, schedules[1].same(dump[1]))

	_ = sc.Stop()
	receiver.wait()
	require.False(t, sc.Running())
}

// TestAddRemoveWhileDispatching adds and removes schedules while the scheduler
// is dispatching a schedule.
func TestAddRemoveWhileDispatching(t *testing.T) {
	sc := NewScheduler(&Options{
		ChannelSize: 0,
		OnChannelFull: OnChannelFull{
			MaxWait: time.Hour,
		},
	})

	// start the scheduler without any listener such that the scheduler will
	// block trying to dispatch
	err := sc.Start()
	require.NoError(t, err)

	startIn := time.Millisecond * 10
	step := time.Millisecond * 10
	now := sc.now()
	schedules := []*Schedule{
		MustSchedule("1", nil, Occur.At(now.Add(startIn))),
		MustSchedule("3", nil, Occur.At(now.Add(startIn+step*2))),
		MustSchedule("2", nil, Occur.At(now.Add(startIn+step))),
	}
	{
		fmt.Println("dispatching 1")
		ok := sc.Add(schedules[0])
		require.True(t, ok)
		time.Sleep(startIn + time.Millisecond)

		dump := sc.Dump()
		require.Equal(t, 1, len(dump))
		require.True(t, schedules[0].same(dump[0]))
	}
	{
		ok := sc.Add(schedules[1])
		require.True(t, ok)
		time.Sleep(startIn + step*2 + time.Millisecond)

		dump := sc.Dump()
		require.Equal(t, 2, len(dump))
		require.True(t, schedules[0].same(dump[0]))
		require.True(t, schedules[1].same(dump[1]))
	}
	{
		ok := sc.Add(schedules[2])
		require.True(t, ok)

		dump := sc.Dump()
		require.Equal(t, 3, len(dump))
		require.True(t, schedules[0].same(dump[0]))
		require.True(t, schedules[2].same(dump[1]))
		require.True(t, schedules[1].same(dump[2]))
	}
	{
		// removing schedule '2' (not dispatching)
		ok := sc.Remove(schedules[2].ID())
		require.True(t, ok)

		dump := sc.Dump()
		require.Equal(t, 2, len(dump))
		require.True(t, schedules[0].same(dump[0]))
		require.True(t, schedules[1].same(dump[1]))
	}
	{
		// removing schedule '1' (dispatching) => '3' becomes dispatching
		fmt.Println("dispatching 3")
		ok := sc.Remove(schedules[0].ID())
		require.True(t, ok)

		dump := sc.Dump()
		require.Equal(t, 1, len(dump))
		// just compare ids since 'details' have changed
		require.Equal(t, schedules[1].id, dump[0].id)
	}
	{
		// removing schedule '3' (dispatching) => nothing dispatched
		ok := sc.Remove(schedules[1].ID())
		require.True(t, ok)

		dump := sc.Dump()
		require.Equal(t, 0, len(dump))
	}

	// verify nothing dispatched
	receiver := NewSimpleReceiver(sc.C())
	receiver.run(nil)
	_ = sc.Stop()
	receiver.wait()

	receiver.assertReceivedIds(t, []string{})
	require.False(t, sc.Running())
}

type SimpleReceiver struct {
	c        chan Entry
	wg       sync.WaitGroup
	received []*Schedule
}

func NewSimpleReceiver(sc chan Entry) *SimpleReceiver {
	return &SimpleReceiver{
		c: sc,
	}
}

func (s *SimpleReceiver) run(callbackFn func(s *Schedule)) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for entry := range s.c {
			e := entry.S()
			s.received = append(s.received, e)
			if callbackFn != nil {
				callbackFn(e)
			}
		}
	}()
}

func (s *SimpleReceiver) wait() {
	s.wg.Wait()
}

func (s *SimpleReceiver) receivedString() []string {
	ret := []string(nil)
	for _, r := range s.received {
		ret = append(ret, r.String())
	}
	return ret
}

func (s *SimpleReceiver) assertReceivedIds(t *testing.T, expectedIds []string) {
	require.Equal(t, len(expectedIds), len(s.received))
	for i, exp := range expectedIds {
		require.Equal(t, exp, string(s.received[i].ID()))
	}
}
