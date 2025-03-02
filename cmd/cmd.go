package cmd

import (
	"github.com/spf13/cobra"

	"github.com/eluv-io/ecobra-go/bflags"
)

func Command() (*cobra.Command, error) {

	sched := bflags.NewBinder(
		NewOpts(),
		&cobra.Command{
			Use:   "sched [flags] -- <command ...>",
			Short: "schedule a command for execution",
			Long: `schedule a command for execution, possibly multiple times.
Command flags must be used to define the start, period and end of execution.
* start option:      --in <duration>     or --at <date>
* recurring options: --every <duration>  or --cron <cron spec>
* ending options:    --during <duration> or --count <int> or --until <date>

The command itself must be specified after -- (if it has its own flags)
UTC dates may be specified using one of the following formats:
  2025-03-03
  2025-03-03Z
  2025-03-03T09:05
  2025-03-03T09:05Z
  2025-03-03T09:05:04
  2025-03-03T09:05:04Z
  2025-03-03T09:05:04.161Z`,
			Args: cobra.MinimumNArgs(1),
			Example: `sched --at 2025-03-02T20:16  --every 1s --during 4s -- ls -l README.md
sched --in 3s --every 1s --during 4s -- ls -l README.md
sched --in 2s --every 1s --count 4 -- ls -l README.md`,
		},
		func(opts *Opts) error {
			opts.InitLog()
			res, err := opts.Sched()
			if err != nil {
				return err
			}

			return opts.Output(res)
		},
		nil)
	if sched.Error != nil {
		return nil, sched.Error
	}

	return sched.Command, nil
}
