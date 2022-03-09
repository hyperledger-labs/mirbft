package main

import (
	"fmt"
	"github.com/ttacon/chalk"
	"strconv"
)

//handles the formatted display of events

//Creates and returns a prefix tag for event display using event metadata
func getMetaTag(eventType string, metadata eventMetadata) string {
	boldGreen := chalk.Green.NewStyle().WithTextStyle(chalk.Bold) //setting font color and style
	boldCyan := chalk.Cyan.NewStyle().WithTextStyle(chalk.Bold)
	return fmt.Sprintf("%s %s",
		boldGreen.Style(fmt.Sprintf("[ Event_%s ]", eventType)),
		boldCyan.Style(fmt.Sprintf("[ Node #%s ] [ Time _%s ] [ Index #%s ]",
			strconv.FormatUint(metadata.nodeID, 10),
			strconv.FormatInt(metadata.time, 10),
			strconv.FormatUint(metadata.index, 10))),
	)
}

//displays the event
func display(eventType string, event string, metadata eventMetadata) {
	whiteText := chalk.White.NewStyle().WithTextStyle(chalk.Bold)
	metaTag := getMetaTag(eventType, metadata)
	fmt.Printf("%s %s \n\n", metaTag, whiteText.Style(event))
}
