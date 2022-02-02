// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package announcer

import (
	"github.com/hyperledger-labs/mirbft/log"
)

// Dummy announcer implementation that only commits the Entry to the local log.
// This is sufficient if all nodes keeping the log are followers in all ordering Segments.
// However, in general, those nodes that are not followers in a Segment should learn about new log Entries from that
// Segment through the Announce mechanism (e.g. by using gossip).
func Announce(entry *log.Entry) {
	log.CommitEntry(entry)
}
