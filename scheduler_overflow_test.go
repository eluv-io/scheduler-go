package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	elog "github.com/eluv-io/log-go"
)

// TestOverflow shows overflow 'issue' with a slow receiver. The receiving
// channel is well configured with a buffer size of 3 slot.
// But the receiver is slow and recurring schedules always rescheduled such that
// they could be flooding the channel (thus preventing other regular schedules
// to be handled: they would time out when being dispatched and finally silently
// discarded with no possibility for the client code to recover). Instead, now
// no attempt to dispatch them is made until the receiver notified handling them.
func TestOverflow(t *testing.T) {
	logger := elog.Get("/")
	logger.SetLevel("debug")

	sc := NewScheduler(&Options{
		ChannelSize: 3,
		OnChannelFull: OnChannelFull{
			MaxWait: defaultMaxWait,
		},
		Logger: logger,
	})

	startIn := time.Millisecond * 200

	schedules := []*Schedule{
		MustSchedule("1", nil, Recur.Every(time.Millisecond*200)),
		MustSchedule("2", nil, Occur.In(startIn)),
		MustSchedule("3", nil, Occur.In(startIn+time.Millisecond)),
	}

	time.AfterFunc(time.Second*5, func() {
		logger.Debug("stopping")
		_ = sc.Stop()
	})

	receiver := NewSimpleReceiver(sc.C())
	receiver.run(func(s *Schedule) {
		logger.Debug("received", "id", s.id)
		switch s.id {
		case "1":
			time.Sleep(time.Second)
		case "2":
			time.Sleep(time.Second * 2)
			ok := s.RescheduleIn(time.Millisecond * 20)
			if sc.Running() {
				require.True(t, ok)
				logger.Debug("rescheduled", "id", s.id)
			}
		case "3":
			time.Sleep(time.Millisecond * 5)
			ok := s.RescheduleIn(time.Millisecond * 20)
			if sc.Running() {
				require.True(t, ok)
				logger.Debug("rescheduled", "id", s.id)
			}
		}
	})

	err := sc.Run(schedules)
	require.NoError(t, err)
	receiver.wait()

	require.False(t, sc.Running())
	overflowed := sc.Outlet()
	recurCount := 0
	regularCount := 0
	for _, s := range overflowed {
		id := string(s.ID())
		if id == "1" {
			recurCount++
		} else {
			regularCount++
		}
	}
	fmt.Println("overflow", "total", len(overflowed),
		"recur", recurCount,
		"regular", regularCount)

	// before fix
	//require.True(t, len(overflowed) > 0)
	//require.True(t, recurCount > 0)
	//require.True(t, regularCount > 0)

	// what we want to have instead
	require.Equal(t, 0, len(overflowed),
		"overflowed schedules: %v", overflowed)
}
