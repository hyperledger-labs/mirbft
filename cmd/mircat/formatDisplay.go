package main

import (
	"fmt"
	"github.com/ttacon/chalk"
	"strconv"
)

func getMetaTag(eventType string, metadata eventMetadata) string {
	boldGreen := chalk.Green.NewStyle().WithTextStyle(chalk.Bold)
	boldBlue := chalk.Blue.NewStyle().WithTextStyle(chalk.Bold)
	return fmt.Sprintf("%s %s",
		boldGreen.Style(fmt.Sprintf("[ Event_%s ]", eventType)),
		boldBlue.Style(fmt.Sprintf("[ Node #%s ] [ Time _%s ] [ Index #%s ]",
			strconv.FormatUint(metadata.nodeID, 10),
			strconv.FormatInt(metadata.time, 10),
			strconv.FormatUint(metadata.index, 10))),
	)
}

func DisplayEvent(eventType string, event string, metadata eventMetadata) {
	whiteText := chalk.White.NewStyle().WithTextStyle(chalk.Bold)
	metaTag := getMetaTag(eventType, metadata)
	fmt.Printf("%s %s \n\n", metaTag, whiteText.Style(event))
}
