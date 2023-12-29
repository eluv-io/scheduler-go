package scheduler_test

import (
	"fmt"
	"sync"
	"time"

	"github.com/eluv-io/scheduler-go"
	"github.com/eluv-io/utc-go"
)

func ExampleNew() {
	sched := scheduler.New()
	_ = sched.Start()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(sched *scheduler.Scheduler) {
		defer wg.Done()
		//for s := range sched.C() {
		//	s.Fn()()
		//}
		for {
			select {
			case s, ok := <-sched.C():
				if !ok {
					return
				}
				// do something with the schedule
				// potentially in a goroutine
				// go func(s scheduler.Schedule) { s.Fn()() }(s)
				s.Fn()()
			}
		}
	}(sched)

	in := time.Millisecond * 200
	sched.At(utc.Now().Add(in), func() { fmt.Println("fired") })
	time.Sleep(time.Millisecond * 205)

	_ = sched.Stop()
	wg.Wait()

	// Output: fired
}
