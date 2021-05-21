package deploytest

import (
	"encoding/binary"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
)

type FakeApp struct {
	Entries []*msgs.QEntry
	CommitC chan *msgs.QEntry
}

func Uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

func (fl *FakeApp) Apply(entry *msgs.QEntry) error {
	if len(entry.Requests) == 0 {
		// this is a no-op batch from a tick, or catchup, ignore it
		return nil
	}
	fl.Entries = append(fl.Entries, entry)
	fl.CommitC <- entry
	return nil
}

func (fl *FakeApp) Snap(*msgs.NetworkState_Config, []*msgs.NetworkState_Client) ([]byte, []*msgs.Reconfiguration, error) {
	return Uint64ToBytes(uint64(len(fl.Entries))), nil, nil
}

func (fl *FakeApp) TransferTo(seqNo uint64, snap []byte) (*msgs.NetworkState, error) {
	return nil, fmt.Errorf("we don't support state transfer in this test (yet)")

}
