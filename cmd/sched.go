package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/eluv-io/errors-go"
	elog "github.com/eluv-io/log-go"
	"github.com/eluv-io/scheduler-go"
	"github.com/eluv-io/utc-go"
)

type Opts struct {
	In          time.Duration `cmd:"flag,in,delay before starting the command"`
	At          string        `cmd:"flag,at,UTC date to start the command"`
	Every       time.Duration `cmd:"flag,every,period to execute the command"`
	CronSpec    string        `cmd:"flag,cron,cron spec defining the period to execute the command"`
	During      time.Duration `cmd:"flag,during,period after which to stop executing the command"`
	Until       string        `cmd:"flag,until,UTC date after which to stop executing the command"`
	Count       int           `cmd:"flag,count,count of command execution"`
	NoStopOnErr bool          `cmd:"flag,no-stop-on-error,do not stop execution when the command reports an error"`
	LogLevel    string        `cmd:"flag,log-level,log level"`
	Command     []string      `cmd:"arg,command,command and args,0"`
	logger      *elog.Log
}

func NewOpts() *Opts {
	return &Opts{
		LogLevel: "error",
	}
}

func (o *Opts) InitLog() {
	o.logger = elog.Get("/")
	o.logger.SetLevel(o.LogLevel)
}

func (o *Opts) Output(res interface{}) error {
	if res != nil {
		fmt.Println(res)
	}
	return nil
}

func (o *Opts) Sched() (interface{}, error) {
	e := errors.TemplateNoTrace("sched", errors.K.Invalid.Default())

	opts := scheduler.NewOptions()
	opts.Logger = o.logger

	sc := scheduler.NewScheduler(opts)
	err := sc.Start()
	if err != nil {
		return nil, e(err)
	}

	var scheduleOpts = []scheduler.ScheduleOpt(nil)
	var starter []scheduler.ScheduleOpt
	{
		if o.In != 0 {
			starter = append(starter, scheduler.Occur.In(o.In))
		}
		if o.At != "" {
			at, err := utc.FromString(o.At)
			if err != nil {
				return nil, e(err)
			}
			starter = append(starter, scheduler.Occur.At(at))
		}
	}
	scheduleOpts = append(scheduleOpts, starter...)

	var repeat []scheduler.ScheduleOpt
	{
		if o.Every != 0 {
			repeat = append(repeat, scheduler.Recur.Every(o.Every))
		}
		if o.CronSpec != "" {
			//cronSched, err := cron.ParseStandard("@every 1s")
			cronSched, err := cron.ParseStandard(o.CronSpec)
			if err != nil {
				return nil, e(err)
			}
			repeat = append(repeat, scheduler.Recur.Cron(cronSched))
		}
		//repeat = append(repeat, scheduler.Recur.NextTime())
	}
	scheduleOpts = append(scheduleOpts, repeat...)

	var until []scheduler.ScheduleOpt
	{
		if o.During != 0 {
			until = append(until, scheduler.Until.Elapsed(o.During))
		}
		if o.Until != "" {
			ut, err := utc.FromString(o.Until)
			if err != nil {
				return nil, e(err)
			}
			until = append(until, scheduler.Until.Date(ut))
		}
		if o.Count > 0 {
			until = append(until, scheduler.Until.Count(o.Count))
		}
	}
	scheduleOpts = append(scheduleOpts, until...)

	start := utc.Zero
	stop := func(count int, det scheduler.Details) bool {
		if o.Count > 0 && count >= o.Count {
			return true
		}
		if o.Until != "" && utc.Now().After(utc.MustParse(o.Until)) {
			return true
		}
		if o.During > 0 && utc.Now().Sub(start) > o.During {
			return true
		}
		if det.Next == utc.Zero {
			return true
		}
		return false
	}

	fn := func() error {
		args := []string(nil)
		if len(o.Command) > 1 {
			args = o.Command[1:]
		}
		cmd := exec.Command(o.Command[0], args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Start()
		if err != nil {
			return err
		}
		err = cmd.Wait()
		if err != nil {
			return err
		}
		return nil
	}

	schedule, err := scheduler.NewSchedule("fn", fn, scheduleOpts...)
	if err != nil {
		return nil, e(err)
	}

	ok := sc.Add(schedule)
	if !ok {
		return nil, e("reason", "schedule rejected")
	}

	sigc := make(chan os.Signal)
	signal.Notify(sigc, syscall.SIGINT)
	go func() {
		for sig := range sigc {
			o.logger.Debug("stop", "signal", sig.String(), "signal#", fmt.Sprintf("%d", sig))
			signal.Reset(syscall.SIGINT)
			_ = sc.Stop()
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(sched *scheduler.Scheduler) {
		defer wg.Done()
		count := 0

	out:
		for {
			select {
			case entry, ok := <-sched.C():
				if !ok {
					return
				}
				s := entry.S()
				count++
				if start == utc.Zero {
					start = s.Details().DispatchedAt
				}
				//fmt.Println("scheduled at", s.Details().ScheduledAt,
				//	"dispatched at", s.Details().DispatchedAt,
				//	"next", s.Details().Next)

				// run the command -> ? potentially in a goroutine
				// go func(s scheduler.Schedule) { s.Fn()() }(s)
				err = s.FnE()()
				if err != nil {
					fmt.Println(err)
					if !o.NoStopOnErr {
						_ = sched.Stop()
						break out
					}
				}

				// stop the scheduler if the entry is not repeatable
				if stop(count, s.Details()) {
					_ = sched.Stop()
				}
			}
		}
	}(sc)
	wg.Wait()

	return nil, nil
}
