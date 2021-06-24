/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deploytest

import (
	"encoding/binary"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
)

// TODO: Make the Fake app only deal with request payloads
//       and hide implementation-specific things like QEntries from it.

type FakeApp struct {
	RequestsProcessed uint64
}

func (fa *FakeApp) Apply(entry *msgs.QEntry) error {
	fa.RequestsProcessed += uint64(len(entry.Requests))
	return nil
}

func (fa *FakeApp) Snapshot(*msgs.NetworkState_Config, []*msgs.NetworkState_Client) ([]byte, []*msgs.Reconfiguration, error) {
	return uint64ToBytes(fa.RequestsProcessed), nil, nil
}

func (fa *FakeApp) TransferTo(seqNo uint64, snap []byte) (*msgs.NetworkState, error) {
	return nil, fmt.Errorf("we don't support state transfer in this test (yet)")
}

func uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}
