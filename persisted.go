/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
)

//go:generate counterfeiter -o mock/storage.go -fake-name Storage . Storage

type Storage interface {
	Load(index uint64) (*pb.Persistent, error)
}

type logEntry struct {
	entry *pb.Persistent
	next  *logEntry
}

type persisted struct {
	offset  uint64
	logHead *logEntry
	logTail *logEntry

	lastCommitted uint64 // Seq

	myConfig *Config
}

func newPersisted(myConfig *Config) *persisted {
	return &persisted{
		myConfig: myConfig,
	}
}

func (p *persisted) appendLogEntry(entry *pb.Persistent) {
	p.offset++
	if p.logHead == nil {
		p.logHead = &logEntry{
			entry: entry,
		}
		p.logTail = p.logHead
	} else {
		p.logTail.next = &logEntry{
			entry: entry,
		}
		p.logTail = p.logTail.next
	}
}

func (p *persisted) addPEntry(pEntry *pb.PEntry) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_PEntry{
			PEntry: pEntry,
		},
	}

	p.appendLogEntry(d)

	return &Actions{
		Persist: []*pb.Persistent{d},
	}

}

func (p *persisted) addQEntry(qEntry *pb.QEntry) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_QEntry{
			QEntry: qEntry,
		},
	}

	p.appendLogEntry(d)

	return &Actions{
		Persist: []*pb.Persistent{d},
	}
}

func (p *persisted) addCEntry(cEntry *pb.CEntry) *Actions {
	if cEntry.NetworkState == nil {
		panic("network config must be set")
	}

	if cEntry.EpochConfig == nil {
		panic("epoch config must be set")
	}

	d := &pb.Persistent{
		Type: &pb.Persistent_CEntry{
			CEntry: cEntry,
		},
	}

	p.appendLogEntry(d)

	return &Actions{
		Persist: []*pb.Persistent{d},
	}
}

func (p *persisted) addSuspect(suspect *pb.Suspect) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_Suspect{
			Suspect: suspect,
		},
	}

	p.appendLogEntry(d)

	return &Actions{
		Persist: []*pb.Persistent{d},
	}
}

func (p *persisted) addEpochChange(epochChange *pb.EpochChange) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_EpochChange{
			EpochChange: epochChange,
		},
	}

	p.appendLogEntry(d)

	return &Actions{
		Persist: []*pb.Persistent{d},
	}
}

func (p *persisted) addNewEpochEcho(newEpochConfig *pb.NewEpochConfig) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_NewEpochEcho{
			NewEpochEcho: newEpochConfig,
		},
	}

	p.appendLogEntry(d)

	return &Actions{
		Persist: []*pb.Persistent{d},
	}
}

func (p *persisted) addNewEpochReady(newEpochConfig *pb.NewEpochConfig) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_NewEpochReady{
			NewEpochReady: newEpochConfig,
		},
	}

	p.appendLogEntry(d)

	return &Actions{
		Persist: []*pb.Persistent{d},
	}
}

func (p *persisted) addNewEpochStart(epochConfig *pb.EpochConfig) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_NewEpochStart{
			NewEpochStart: epochConfig,
		},
	}

	p.appendLogEntry(d)

	return &Actions{
		Persist: []*pb.Persistent{d},
	}
}

func (p *persisted) setLastCommitted(seqNo uint64) {
	if p.lastCommitted+1 != seqNo {
		panic(fmt.Sprintf("dev sanity test, remove me: lastCommitted=%d >= seqNo=%d", p.lastCommitted, seqNo))
	}

	p.lastCommitted = seqNo
}

func (p *persisted) truncate(lowWatermark uint64) {
	for head := p.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_PEntry:
			if d.PEntry.SeqNo > lowWatermark {
				return
			}
		case *pb.Persistent_QEntry:
			if d.QEntry.SeqNo > lowWatermark {
				return
			}
		case *pb.Persistent_CEntry:
			p.logHead = head
			if d.CEntry.SeqNo >= lowWatermark {
				return
			}
		default:
			// panic("unrecognized data type")
		}
	}
}

func (p *persisted) constructEpochChange(newEpoch uint64) *pb.EpochChange {
	epochChange := &pb.EpochChange{
		NewEpoch: newEpoch,
	}

	var logEpoch uint64
	for head := p.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_PEntry:
			epochChange.PSet = append(epochChange.PSet, &pb.EpochChange_SetEntry{
				Epoch:  logEpoch,
				SeqNo:  d.PEntry.SeqNo,
				Digest: d.PEntry.Digest,
			})
		case *pb.Persistent_QEntry:
			epochChange.QSet = append(epochChange.QSet, &pb.EpochChange_SetEntry{
				Epoch:  logEpoch,
				SeqNo:  d.QEntry.SeqNo,
				Digest: d.QEntry.Digest,
			})
		case *pb.Persistent_CEntry:
			epochChange.Checkpoints = append(epochChange.Checkpoints, &pb.Checkpoint{
				SeqNo: d.CEntry.SeqNo,
				Value: d.CEntry.CheckpointValue,
			})
		case *pb.Persistent_EpochChange:
			if logEpoch+1 != d.EpochChange.NewEpoch {
				panic("dev sanity test")
			}
			logEpoch = d.EpochChange.NewEpoch
		}
	}

	return epochChange
}
