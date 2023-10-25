# `Go scheduler`

<p align="center" width="100%">
    <img alt="hourglass" width="20%" src="hourglass.png"> 
</p>

`scheduler` represents sequence(s) of planned events or `schedules` dispatched on the scheduler's notification channel C. <br/>
When a Schedule's time expires, the schedule is sent on C.

### Sample Usage

```
	// initialize a scheduler
	sched := scheduler.New()
	_ = sched.Start()
	
	// start a receiver 
	go func(sched *scheduler.Scheduler) {
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
				
			// other select cases	
			case ...	
			}
		}
	}(sched)

	// stop the scheduler when done	
	_ = sched.Stop()
```

The scheduler can be started either empty (using function `Start()` as above) or with known schedules using function `Run(schedules []*Schedule)`.

Once the scheduler is running, schedules can be added or removed. The scheduler can also return a dump of the current schedules.

### Options

The default scheduler (using `New`) is initialized with a buffered channel of size 1 and configured such that if the 
receiver is slow handling schedules and the buffer is full, the scheduler won't skip the schedule and will retry once 
after 10ms.

These options are configurable through `Options` passed to function `NewScheduler`. <br/>
Below an `Options` example with a zero channel size and skipping schedules when the channel is full:

```
	return &Options{
		ChannelSize: 0,
		OnChannelFull: OnChannelFull{
			SkipDispatch: true,
		},
	}
```

### Schedules

The scheduler has helper functions to add simple schedules: 
* firing at a given date:  `Scheduler.At(at utc.UTC, o interface{}) ScheduleID`
* firing after a given duration: `Scheduler.In(d time.Duration, o interface{}) ScheduleID`

More sophisticated schedules are added via function `Scheduler.Add(sc *Schedule) bool`.

### Building schedules

A `Schedule` is built via function `NewSchedule(id string, o interface{}, opts ...ScheduleOpt) (*Schedule, error)`

#### Occur

`scheduler.Occur` are options to build a one time schedule or the initial date of a recurrent schedule:
* `At( date )` specifies the date when to fire the schedule.
* `In( duration )` specifies a duration after which the schedule must be fired. The exact date is computed when the scheduler receives the schedule. 

#### Recurrent schedules

There are two manners to make a recurrent schedule:
* call function `Reschedule(t utc.UTC)` on the received schedule in the receiver code, or
* use a `Recur` option when building the schedule. **Note** that for a schedule built using a `Recur` option, calling `Reschedule` has no effect. 

Using `Reschedule(t utc.UTC)` might be preferred when a function is attached to the schedule and the goal is to plan the 
next execution _after_ the attached function has run. <br/>
By contrast, when using a `Recur` option, the next firing date is computed immediately after the schedule was sent to 
the notification channel. 

`scheduler.Recur` options to build recurring schedules:
* `Next(n Nexter)` uses a `Nexter` object to compute the next firing date.
* `Every(d time.Duration)` makes the schedule fire every given duration.
* `NextTime(fn func(t time.Time) time.Time)` computes the next time the schedule must fire
* `Cron(schedule cron.Schedule)` uses a [cron/v3](https://github.com/robfig/cron/blob/master/cron.go) schedule object.
 
Note that a `Nexter` can also be built around [cronexpr](https://github.com/angadn/cronexpr)

The first date of a recurring schedule is computed via the recurring option when the scheduler receives the schedule 
unless an initial date was given via an `Occur` option. 

#### Limiting recurrent schedules

`scheduler.Until` options provide conditions to tell the scheduler to stop rescheduling schedules built via a `Recur` option:
* `Count(count int)` limits the number of times the schedule is fired.
* `Date(u utc.UTC)` specifies a maximum date for the schedule.
* `Elapsed(d time.Duration)` is the maximum duration of the schedule after first dispatching.

If both `Date` and `Elapsed` are specified, the earliest resulting date is used.

### Other projects in the same area

* A cron library for go: [cron/V3](https://github.com/robfig/cron/)   
* A Golang Job Scheduling Package: [gocron](https://github.com/go-co-op/gocron) ([original project](https://github.com/jasonlvhit/gocron)) 
