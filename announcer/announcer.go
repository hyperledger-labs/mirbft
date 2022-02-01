package announcer

import (
	"github.ibm.com/mir-modular/log"
)

// Dummy announcer implementation that only commits the Entry to the local log.
// This is sufficient if all nodes keeping the log are followers in all ordering Segments.
// However, in general, those nodes that are not followers in a Segment should learn about new log Entries from that
// Segment through the Announce mechanism (e.g. by using gossip).
func Announce(entry *log.Entry) {
	log.CommitEntry(entry)
}
