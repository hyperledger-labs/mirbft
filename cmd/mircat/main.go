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

var (
	eventList = []string{
		"Initialize",
		"Tick",
		"Deliver",
		"WalAppend",
		"HashResult",
		"WalEntry",
		"WalTruncate",
		"ActionsReceived",
		"HashRequest",
		"MessageReceived",
		"Iss",
		"VerifyRequestSig",
		"RequestSigVerified",
		"StoreVerifiedRequest",
		"AppSnapshotRequest",
		"AppSnapshot",
		"SendMessage",
		"RequestReady",
		"StoreDummyRequest",
		"AnnounceDummyBatch",
		"PersistDummyBatch",
	}
	issEventList = []string{
		"PersistCheckpoint",
		"StableCheckpoint",
		"PersistStableCheckpoint",
		"Sb",
	}
)

func checkboxes(label string, opts []string) []string {
	res := []string{}
	prompt := &survey.MultiSelect{
		Message: label,
		Options: opts,
	}
	survey.AskOne(prompt, &res)

	return res
}

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
	args.includedEvents = checkboxes(
		"Please select the events", eventList)

	if IncludedIn("Iss", args.includedEvents) {
		args.includedIssEvents = checkboxes(
			"Please select the iss events", issEventList)
	}
	err = ReadEvent(args)
	if err != nil {
		kingpin.Fatalf("Error Reading Event", err)
	}

}
