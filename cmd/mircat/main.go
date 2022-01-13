package main

import (
	"github.com/AlecAivazis/survey/v2"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
)

type arguments struct {
	srcFile           *os.File
	includedEvents    []string
	includedIssEvents []string
}

//prompts users with a list of available events to select from
func checkboxes(label string, opts []string) []string {
	res := []string{}
	prompt := &survey.MultiSelect{
		Message: label,
		Options: opts,
	}
	survey.AskOne(prompt, &res)

	return res
}

//parse the command line arguments
func parseArgs(args []string) (*arguments, error) {
	if len(args) == 0 {
		return nil, errors.Errorf("required input \" --src <Src_File> \" not found !")
	}

	app := kingpin.New("mircat", "Utility for processing Mir state event logs.")
	src := app.Flag("src", "The input file to read (defaults to stdin).").Default(os.Stdin.Name()).File()
	_, err := app.Parse(args)
	if err != nil {
		return nil, err
	}

	return &arguments{
		srcFile: *src,
	}, nil
}

func main() {
	kingpin.Version("0.0.1")
	args, err := parseArgs(os.Args[1:])

	if err != nil {
		kingpin.Fatalf("Cannot parse given argument", err)
	}
	err, eventList, issEventList := getEventList(args.srcFile) //gets the list of events from the eventlog
	if err != nil {
		kingpin.Fatalf("Cannot retrieve events from src file", err)
	}
	args.includedEvents = checkboxes( //gets the list of events selected by the user
		"Please select the events", eventList)

	if includedIn("Iss", args.includedEvents) {
		args.includedIssEvents = checkboxes( //gets the list of Iss events selected by the user
			"Please select the iss events", issEventList)
	}
	err = processEvent(args)
	if err != nil {
		kingpin.Fatalf("Error Reading Event", err)
	}

}
