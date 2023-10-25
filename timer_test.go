package scheduler

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/eluv-io/log-go"
)

func TestTimerStop(t *testing.T) {
	type testCase struct {
		descr            string
		d                time.Duration
		wantFired        bool
		wantStop         bool
		wantDrainTimeout bool
	}
	for _, tc := range []*testCase{
		{descr: "not run", d: time.Hour, wantFired: false, wantStop: true},
		{descr: "has run", d: time.Millisecond * 2, wantFired: true, wantStop: false, wantDrainTimeout: true},
		//{descr: "racy", d: (time.Millisecond * 12) + time.Microsecond*2, wantFired: false, wantStop: true},
	} {
		fired := false
		timer := time.NewTimer(tc.d)
		time.Sleep(time.Millisecond * 10)
		select {
		case <-timer.C:
			fired = true
		default:
		}
		b := timer.Stop()
		require.Equal(t, tc.wantFired, fired, "fired "+tc.descr)
		require.Equal(t, tc.wantStop, b, "stopped "+tc.descr)

		t2 := time.NewTimer(time.Second)
		drainTimedOut := false
		if !b {
			select {
			case <-timer.C:
			case <-t2.C:
				drainTimedOut = true
			}
			require.Equal(t, tc.wantDrainTimeout, drainTimedOut, tc.descr)
		}
	}

}

func WaitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	start := time.Now()
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	timer := time.NewTimer(timeout)
	select {
	case <-c:
		if log.IsDebug() {
			log.Debug("wait finished before timeout", "timeout", timeout, "actual_duration", time.Now().Sub(start))
		}
		if !timer.Stop() {
			<-timer.C
		}
		return false // completed normally
	case <-timer.C:
		if log.IsInfo() {
			log.Info("wait timed out!", "timeout", timeout, "actual_duration", time.Now().Sub(start))
		}
		return true // timed out
	}
}
