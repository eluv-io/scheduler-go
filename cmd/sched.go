package cmd

import (
	"fmt"
	"time"

	elog "github.com/eluv-io/log-go"
)

type Opts struct {
	In          time.Duration `cmd:"flag,in,delay before starting the command,i"`
	At          string        `cmd:"flag,at,UTC date to start the command,a"`
	Every       time.Duration `cmd:"flag,every,period to execute the command,e"`
	CronSpec    string        `cmd:"flag,cron,cron spec defining the period to execute the command,s"`
	During      time.Duration `cmd:"flag,during,period after which to stop executing the command,d"`
	Until       string        `cmd:"flag,until,UTC date after which to stop executing the command,u"`
	Count       int           `cmd:"flag,count,count of command execution,c"`
	NoStopOnErr bool          `cmd:"flag,no-stop-on-error,do not stop execution when the command reports an error,n"`
	Async       bool          `cmd:"flag,async,execute command asynchronously,x"`
	Parallel    int           `cmd:"flag,parallel,limit count of parallel execution with --async (0: no limit),p"`
	LogLevel    string        `cmd:"flag,log-level,log level,l"`
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
