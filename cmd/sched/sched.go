package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/eluv-io/log-go"
	"github.com/eluv-io/scheduler-go/cmd"
)

func goSshVersion() string {
	return "N/A"
}

func root() (*cobra.Command, error) {
	ver := goSshVersion()

	root, err := cmd.Command()
	if err != nil {
		return nil, err
	}
	root.Version = ver
	root.CompletionOptions.DisableDefaultCmd = true

	return root, nil
}

func main() {
	cmdRoot, err := root()
	if err != nil {
		// initialisation error won't be reported, print it here
		fmt.Println(err)
		os.Exit(1)
	}

	err = cmdRoot.Execute()
	if err != nil {
		if !cmdRoot.SilenceUsage {
			// report the error if the command was not started yet (SilenceUsage is false).
			log.Error(err.Error())
		}
		os.Exit(1)
	}
}
