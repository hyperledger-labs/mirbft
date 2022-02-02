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

package log

import (
	"bytes"
	"encoding/base64"
	"runtime/debug"
	"testing"
	"time"

	pb "github.com/hyperledger-labs/protobufs"
)

var (
	s1, s2 chan *Entry
	e1     = make(chan *Entry, 1)
)

func init() {
	s1 = Entries()
	s2 = Entries()
	payload1, _ := base64.RawStdEncoding.DecodeString("Entry 0")
	payload2, _ := base64.RawStdEncoding.DecodeString("Entry 2")
	CommitEntry(&Entry{
		Sn: 0,
		Batch: &pb.Batch{
			Requests: []*pb.ClientRequest{{
				RequestId: &pb.RequestID{
					ClientId: 0,
					ClientSn: 0,
				},
				Payload:   payload1,
				Signature: nil,
			}},
		},
	})

	CommitEntry(&Entry{
		Sn: 2,
		Batch: &pb.Batch{
			Requests: []*pb.ClientRequest{{
				RequestId: &pb.RequestID{
					ClientId: 0,
					ClientSn: 2,
				},
				Payload:   payload2,
				Signature: nil,
			}},
		},
	})
}

func TestLog(t *testing.T) {
	payload1, _ := base64.RawStdEncoding.DecodeString("Entry 0")
	payload2, _ := base64.RawStdEncoding.DecodeString("Entry 2")

	if bytes.Compare(GetEntry(0).Batch.Requests[0].Payload, payload1) != 0 {
		t.Error()
	}

	if GetEntry(1) != nil {
		t.Error()
	}

	if bytes.Compare(GetEntry(2).Batch.Requests[0].Payload, payload2) != 0 {
		t.Error()
	}
}

func TestLog_WaitForEntry(t *testing.T) {

	WaitForEntry(0) // Must return.

	go func() {
		WaitForEntry(1) // Must not run yet.
		e1 <- GetEntry(1)
	}()

	time.Sleep(time.Second) // Give the goroutine some time to run.
	checkChannel(t, e1, nil, true)
}

func TestLog_Subscribe(t *testing.T) {
	payload0, _ := base64.RawStdEncoding.DecodeString("Entry 0")
	payload1, _ := base64.RawStdEncoding.DecodeString("Entry 1")
	payload2, _ := base64.RawStdEncoding.DecodeString("Entry 2")

	checkChannel(t, s1, payload0, false)
	checkChannel(t, s1, nil, true)

	CommitEntry(&Entry{
		Sn: 1,
		Batch: &pb.Batch{
			Requests: []*pb.ClientRequest{{
				RequestId: &pb.RequestID{
					ClientId: 0,
					ClientSn: 1,
				},
				Payload:   payload1,
				Signature: nil,
			}},
		},
	})

	checkChannel(t, s1, payload1, false)
	checkChannel(t, s1, payload2, false)
	checkChannel(t, s1, nil, true)

	checkChannel(t, s2, payload0, false)
	checkChannel(t, s2, payload1, false)
	checkChannel(t, s2, payload2, false)
	checkChannel(t, s2, nil, true)
}

func TestLog_WaitForEntryCont(t *testing.T) {
	payload1, _ := base64.RawStdEncoding.DecodeString("Entry 1")

	// Sleep a bit, just to be sure that the anonymous goroutine from the first part of this test had time to run.
	time.Sleep(time.Second)

	// Entry 1 has been pushed by previous test, so waiting must have finished.
	checkChannel(t, e1, payload1, false)
}

func checkChannel(t *testing.T, ch chan *Entry, val []byte, mustBlock bool) {
	select {
	case e := <-ch:
		if mustBlock {
			t.Errorf("Channel expected to block on reading, but value %s was read.", e.Batch.Requests[0].Payload)
		} else if bytes.Compare(e.Batch.Requests[0].Payload, val) != 0 {
			t.Errorf("Unexpected value in channel: %s (expected %s)", e.Batch.Requests[0].Payload, val)
		}
	default:
		if !mustBlock {
			t.Error("Channel must not be empty here.")
			debug.PrintStack()
		}
	}
}
