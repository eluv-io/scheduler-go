package scheduler

import (
	"fmt"
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
	receiver.run(func(Schedule) {
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
}

func TestAdd(t *testing.T) {
	sc := New()

	receiver := NewSimpleReceiver(sc.C())
	receiver.run(func(Schedule) {
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
	receiver.run(func(s Schedule) {
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
	receiver.run(func(s Schedule) {
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
}

func TestChannelFull(t *testing.T) {
	for _, skipDispatch := range []bool{true, false} {
		sc := NewScheduler(&Options{
			ChannelSize: 0,
			OnChannelFull: OnChannelFull{
				SkipDispatch: skipDispatch,
				MaxRetries:   1,
				RetryAfter:   time.Millisecond * 10,
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
		receiver.run(func(s Schedule) {
			if len(receiver.received) == 1 {
				// pause receiving after the first one received
				time.Sleep(time.Millisecond * 12)
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

		fmt.Println("skipDispatch: ", skipDispatch)
		fmt.Println(strings.Join(receiver.receivedString(), "\n"))
		if skipDispatch {
			receiver.assertReceivedIds(t, []string{"1", "3"})
		} else {
			receiver.assertReceivedIds(t, []string{"1", "2", "3"})
		}
	}
}

func TestDispatchBlocking(t *testing.T) {

	sc := NewScheduler(&Options{
		ChannelSize: 0,
		OnChannelFull: OnChannelFull{
			SkipDispatch: false,
			Block:        true,
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
	receiver.run(func(s Schedule) {
		time.Sleep(time.Millisecond * 100)
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
}

func TestReschedule(t *testing.T) {
	sc := New()

	delay := time.Millisecond * 200
	schedules := []*Schedule{
		MustSchedule("1", nil, Occur.In(delay)),
	}

	receiver := NewSimpleReceiver(sc.C())
	receiver.run(func(s Schedule) {
		if len(receiver.received) == 3 {
			scheds, err := sc.DumpStop()
			require.NoError(t, err)
			require.Equal(t, 0, len(scheds))
			return
		}
		s.RescheduleAt(utc.Now().Add(delay))
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
}

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
}

type SimpleReceiver struct {
	c        chan Schedule
	wg       sync.WaitGroup
	received []Schedule
}

func NewSimpleReceiver(sc chan Schedule) *SimpleReceiver {
	return &SimpleReceiver{
		c: sc,
	}
}

func (s *SimpleReceiver) run(callbackFn func(s Schedule)) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for e := range s.c {
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
