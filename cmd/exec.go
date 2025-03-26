package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/robfig/cron/v3"

	"github.com/eluv-io/errors-go"
	"github.com/eluv-io/scheduler-go"
	"github.com/eluv-io/utc-go"
)

func (o *Opts) scheduleOpts() ([]scheduler.ScheduleOpt, error) {
	e := errors.Template("scheduleOpts", errors.K.Invalid)

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
	return scheduleOpts, nil
}

func (o *Opts) execCommand() error {
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
	return cmd.Wait()
}

func (o *Opts) SchedExec() (interface{}, error) {
	e := errors.TemplateNoTrace("sched", errors.K.Invalid.Default())

	opts := scheduler.NewOptions()
	opts.ChannelSize = 0
	opts.Logger = o.logger

	sc := scheduler.NewScheduler(opts)
	err := sc.Start()
	if err != nil {
		return nil, e(err)
	}
	defer func() {
		if err != nil {
			_ = sc.Stop()
		}
	}()

	sigc := make(chan os.Signal)
	signal.Notify(sigc, syscall.SIGINT)
	go func() {
		for sig := range sigc {
			o.logger.Debug("stop", "signal", sig.String(), "signal#", fmt.Sprintf("%d", sig))
			signal.Reset(syscall.SIGINT)
			_ = sc.Stop()
		}
	}()

	stopFn := func(start utc.UTC, count int, s *scheduler.Schedule) bool {
		if o.Count > 0 && count >= o.Count {
			return true
		}
		if o.Until != "" && utc.Now().After(utc.MustParse(o.Until)) {
			return true
		}
		if o.During > 0 && utc.Now().Sub(start) > o.During {
			return true
		}
		if s.Details().Next == utc.Zero {
			return true
		}
		return false
	}

	runFn := func(s *scheduler.Schedule) bool {
		err := s.FnE()()
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr,
				"id", s.ID(),
				"dispatched at", s.Details().DispatchedAt, err)
			if !o.NoStopOnErr {
				return false
			}
		}
		return true
	}

	scheduleOpts, err := o.scheduleOpts()
	if err != nil {
		return nil, e(err)
	}

	schedule, err := scheduler.NewSchedule("fn", o.execCommand, scheduleOpts...)
	if err != nil {
		return nil, e(err)
	}

	ok := sc.Add(schedule)
	if !ok {
		return nil, e("reason", "schedule rejected")
	}

	executor, err := scheduler.NewSchedExec(
		o.Async,
		o.Parallel,
		runFn,
		stopFn,
		o.logger)
	if err != nil {
		return nil, e(err)
	}
	executor.RunWait(sc)

	return nil, nil
}
