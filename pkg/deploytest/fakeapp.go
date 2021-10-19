/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deploytest

import (
	"encoding/binary"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
)

// FakeApp represents a dummy stub application used for testing only.
type FakeApp struct {

	// The state of the FakeApp only consists of a counter of processed requests.
	RequestsProcessed uint64
}

// Apply
func (fa *FakeApp) Apply(batch *requestpb.Batch) error {
	for range batch.Requests {
		fa.RequestsProcessed++
		fmt.Printf("Processed requests: %d\n", fa.RequestsProcessed)
	}
	return nil
}

func (fa *FakeApp) Snapshot() ([]byte, error) {
	return uint64ToBytes(fa.RequestsProcessed), nil
}

func (fa *FakeApp) RestoreState(snapshot []byte) error {
	return fmt.Errorf("we don't support state transfer in this test (yet)")
}

func uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}
