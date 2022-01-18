package main

import (
	"fmt"
	"github.com/AlecAivazis/survey/v2"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"sort"
)

// mircat is a tool for reviewing Mir state machine recordings.
// It understands the format encoded via github.com/hyperledger-labs/mirbft/eventlog
// and is able to parse and filter these log files based on the events.

type arguments struct {
	// File containing the event log to read.
	srcFile *os.File

	// Events selected by the user for displaying.
	includedEvents map[string]struct{}

	// If ISS Events have been selected for displaying, this variable contains the types of ISS events to be displayed.
	includedIssEvents map[string]struct{}
}

// Converts a set of strings (represented as a map) to a list.
// Returns a slice containing all the keys present in the given set.
// toList is used to convert sets to a format used by the survey library.
func toList(set map[string]struct{}) []string {
	list := make([]string, 0, len(set))
	for item, _ := range set {
		list = append(list, item)
	}
	sort.Strings(list)
	return list
}

// Converts a list of strings to a set (represented as a map).
// Returns a map of empty structs with one entry for each unique item of the given list (the item being the map key).
// toSet is used to convert lists produced by the survey library to sets for easier lookup.
func toSet(list []string) map[string]struct{} {
	set := make(map[string]struct{})
	for _, item := range list {
		set[item] = struct{}{}
	}
	return set
}

// Prompts users with a list of available Events to select from.
// Returns a set of selected Events.
func checkboxes(label string, opts map[string]struct{}) map[string]struct{} {

	// Use survey library to get a list selected event names.
	selected := make([]string, 0)
	prompt := &survey.MultiSelect{
		Message: label,
		Options: toList(opts),
	}
	survey.AskOne(prompt, &selected)

	return toSet(selected)
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

	// Parse command line arguments
	kingpin.Version("0.0.1")
	args, err := parseArgs(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("Cannot parse given argument", err)
	}

	// Scan the event log and collect all occurring event types.
	allEvents, allISSEvents, totalEvents, err := getEventList(args.srcFile)
	if err != nil {
		kingpin.Fatalf("Cannot retrieve events from src file", err)
	}
	fmt.Printf("Total number of events found: %d\n", totalEvents)

	// Have the user select a subset of events to include in the output
	args.includedEvents = checkboxes(
		"Please select the events", allEvents)

	// If any ISS events occur in the event log, have the user select which of those should be included in the output.
	if _, ok := args.includedEvents["Iss"]; ok {
		args.includedIssEvents = checkboxes(
			"Please select the iss events", allISSEvents)
	}

	// Process all events.
	err = processEvents(args)
	if err != nil {
		kingpin.Fatalf("Error Processing Events", err)
	}

}
