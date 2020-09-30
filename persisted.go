/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type logEntry struct {
	index uint64
	entry *pb.Persistent
	next  *logEntry
}

type persisted struct {
	nextIndex uint64
	logHead   *logEntry
	logTail   *logEntry

	logger Logger
}

func newPersisted(logger Logger) *persisted {
	return &persisted{
		logger: logger,
	}
}

func (p *persisted) appendInitialLoad(entry *WALEntry) {
	if p.logHead == nil {
		p.nextIndex = entry.Index
		p.logHead = &logEntry{
			index: entry.Index,
			entry: entry.Data,
		}
		p.logTail = p.logHead
	} else {
		p.logTail.next = &logEntry{
			index: entry.Index,
			entry: entry.Data,
		}
		p.logTail = p.logTail.next
	}
	if p.nextIndex != entry.Index {
		panic(fmt.Sprintf("WAL indexes out of order! Expected %d got %d", p.nextIndex, entry.Index))
	}
	p.nextIndex = entry.Index + 1
}

func (p *persisted) appendLogEntry(entry *pb.Persistent) *Actions {
	p.logTail.next = &logEntry{
		index: p.nextIndex,
		entry: entry,
	}
	p.logTail = p.logTail.next
	result := (&Actions{}).persist(p.nextIndex, entry)
	p.nextIndex++
	return result
}

func (p *persisted) addPEntry(pEntry *pb.PEntry) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_PEntry{
			PEntry: pEntry,
		},
	}

	return p.appendLogEntry(d)
}

func (p *persisted) addQEntry(qEntry *pb.QEntry) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_QEntry{
			QEntry: qEntry,
		},
	}

	return p.appendLogEntry(d)
}

func (p *persisted) addCEntry(cEntry *pb.CEntry) *Actions {
	if cEntry.NetworkState == nil {
		panic("network config must be set")
	}

	d := &pb.Persistent{
		Type: &pb.Persistent_CEntry{
			CEntry: cEntry,
		},
	}

	return p.appendLogEntry(d)
}

func (p *persisted) addSuspect(suspect *pb.Suspect) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_Suspect{
			Suspect: suspect,
		},
	}

	return p.appendLogEntry(d)
}

func (p *persisted) addEpochChange(epochChange *pb.EpochChange) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_EpochChange{
			EpochChange: epochChange,
		},
	}

	return p.appendLogEntry(d)
}

func (p *persisted) addNewEpochEcho(newEpochConfig *pb.NewEpochConfig) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_NewEpochEcho{
			NewEpochEcho: newEpochConfig,
		},
	}

	return p.appendLogEntry(d)
}

func (p *persisted) addNewEpochReady(newEpochConfig *pb.NewEpochConfig) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_NewEpochReady{
			NewEpochReady: newEpochConfig,
		},
	}

	return p.appendLogEntry(d)
}

func (p *persisted) addNewEpochStart(epochConfig *pb.EpochConfig) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_NewEpochStart{
			NewEpochStart: epochConfig,
		},
	}

	return p.appendLogEntry(d)
}

func (p *persisted) truncate(lowWatermark uint64) *Actions {
	var lastCEntry *logEntry
	for logEntry := p.logHead; logEntry != nil; logEntry = logEntry.next {
		switch d := logEntry.entry.Type.(type) {
		case *pb.Persistent_PEntry:
			if d.PEntry.SeqNo <= lowWatermark {
				continue
			}
		case *pb.Persistent_QEntry:
			if d.QEntry.SeqNo <= lowWatermark {
				continue
			}
		case *pb.Persistent_CEntry:
			lastCEntry = logEntry
			if d.CEntry.SeqNo < lowWatermark {
				continue
			}
		default:
			continue
		}

		p.logHead = lastCEntry
		return &Actions{
			WriteAhead: []*Write{
				{
					Truncate: &lastCEntry.index,
				},
			},
		}
	}

	return &Actions{}
}

func (p *persisted) constructEpochChange(newEpoch uint64) *pb.EpochChange {
	epochChange := &pb.EpochChange{
		NewEpoch: newEpoch,
	}

	// To avoid putting redundant entries into the pSet, we count
	// how many are in the log for each sequence so that we may
	// skip all but the last entry for each sequence number
	pSkips := map[uint64]int{}
	for head := p.logHead; head != nil; head = head.next {
		d, ok := head.entry.Type.(*pb.Persistent_PEntry)
		if !ok {
			continue
		}
		count := pSkips[d.PEntry.SeqNo]
		pSkips[d.PEntry.SeqNo] = count + 1
	}

	var logEpoch *uint64
	fmt.Printf("JKY: Looping through log\n")
	for head := p.logHead; head != nil; head = head.next {
		fmt.Printf("  JKY: log entry of type %T\n", head.entry.Type)
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_PEntry:
			count := pSkips[d.PEntry.SeqNo]
			if count != 1 {
				pSkips[d.PEntry.SeqNo] = count - 1
				continue
			}
			epochChange.PSet = append(epochChange.PSet, &pb.EpochChange_SetEntry{
				Epoch:  *logEpoch,
				SeqNo:  d.PEntry.SeqNo,
				Digest: d.PEntry.Digest,
			})
		case *pb.Persistent_QEntry:
			epochChange.QSet = append(epochChange.QSet, &pb.EpochChange_SetEntry{
				Epoch:  *logEpoch,
				SeqNo:  d.QEntry.SeqNo,
				Digest: d.QEntry.Digest,
			})
		case *pb.Persistent_CEntry:
			epochChange.Checkpoints = append(epochChange.Checkpoints, &pb.Checkpoint{
				SeqNo: d.CEntry.SeqNo,
				Value: d.CEntry.CheckpointValue,
			})
			if logEpoch == nil {
				logEpoch = &d.CEntry.CurrentEpoch
			}
		case *pb.Persistent_EpochChange:
			if *logEpoch+1 != d.EpochChange.NewEpoch {
				panic(fmt.Sprintf("dev sanity test: expected epochChange target %d to be exactly one more than our current epoch %d", d.EpochChange.NewEpoch, *logEpoch))
			}
			logEpoch = &d.EpochChange.NewEpoch
		}
	}

	return epochChange
}
