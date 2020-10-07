/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type logIterator struct {
	onQEntry   func(*pb.QEntry)
	onPEntry   func(*pb.PEntry)
	onCEntry   func(*pb.CEntry)
	onNEntry   func(*pb.NEntry)
	onFEntry   func(*pb.FEntry)
	onECEntry  func(*pb.ECEntry)
	onSuspect  func(*pb.Suspect)
	shouldExit func() bool
	// TODO, suspect_ready
}

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

func (p *persisted) addECEntry(ecEntry *pb.ECEntry) *Actions {
	d := &pb.Persistent{
		Type: &pb.Persistent_ECEntry{
			ECEntry: ecEntry,
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

func (p *persisted) iterate(li logIterator) {
	for logEntry := p.logHead; logEntry != nil; logEntry = logEntry.next {
		switch d := logEntry.entry.Type.(type) {
		case *pb.Persistent_PEntry:
			if li.onPEntry != nil {
				li.onPEntry(d.PEntry)
			}
		case *pb.Persistent_QEntry:
			if li.onQEntry != nil {
				li.onQEntry(d.QEntry)
			}
		case *pb.Persistent_CEntry:
			if li.onCEntry != nil {
				li.onCEntry(d.CEntry)
			}
		case *pb.Persistent_NEntry:
			if li.onNEntry != nil {
				li.onNEntry(d.NEntry)
			}
		case *pb.Persistent_FEntry:
			if li.onFEntry != nil {
				li.onFEntry(d.FEntry)
			}
		case *pb.Persistent_ECEntry:
			if li.onECEntry != nil {
				li.onECEntry(d.ECEntry)
			}
		case *pb.Persistent_Suspect:
			if li.onSuspect != nil {
				li.onSuspect(d.Suspect)
			}
			// TODO, suspect_ready
		default:
			panic(fmt.Sprintf("unsupported log entry type '%T'", logEntry.entry.Type))
		}

		if li.shouldExit != nil && li.shouldExit() {
			break
		}
	}
}

func (p *persisted) constructEpochChange(newEpoch uint64) *pb.EpochChange {
	newEpochChange := &pb.EpochChange{
		NewEpoch: newEpoch,
	}

	// To avoid putting redundant entries into the pSet, we count
	// how many are in the log for each sequence so that we may
	// skip all but the last entry for each sequence number
	pSkips := map[uint64]int{}
	var logEpoch *uint64
	p.iterate(logIterator{
		shouldExit: func() bool {
			return logEpoch != nil && *logEpoch >= newEpoch
		},
		onPEntry: func(pEntry *pb.PEntry) {
			count := pSkips[pEntry.SeqNo]
			pSkips[pEntry.SeqNo] = count + 1
		},
		onNEntry: func(nEntry *pb.NEntry) {
			logEpoch = &nEntry.EpochConfig.Number
		},
		onFEntry: func(fEntry *pb.FEntry) {
			logEpoch = &fEntry.EndsEpochConfig.Number
		},
	})

	logEpoch = nil
	p.iterate(logIterator{
		shouldExit: func() bool {
			return logEpoch != nil && *logEpoch >= newEpoch
		},
		onPEntry: func(pEntry *pb.PEntry) {
			count := pSkips[pEntry.SeqNo]
			if count != 1 {
				pSkips[pEntry.SeqNo] = count - 1
				return
			}
			newEpochChange.PSet = append(newEpochChange.PSet, &pb.EpochChange_SetEntry{
				Epoch:  *logEpoch,
				SeqNo:  pEntry.SeqNo,
				Digest: pEntry.Digest,
			})
		},
		onQEntry: func(qEntry *pb.QEntry) {
			newEpochChange.QSet = append(newEpochChange.QSet, &pb.EpochChange_SetEntry{
				Epoch:  *logEpoch,
				SeqNo:  qEntry.SeqNo,
				Digest: qEntry.Digest,
			})
		},
		onNEntry: func(nEntry *pb.NEntry) {
			logEpoch = &nEntry.EpochConfig.Number
		},
		onFEntry: func(fEntry *pb.FEntry) {
			logEpoch = &fEntry.EndsEpochConfig.Number
		},
		onCEntry: func(cEntry *pb.CEntry) {
			newEpochChange.Checkpoints = append(newEpochChange.Checkpoints, &pb.Checkpoint{
				SeqNo: cEntry.SeqNo,
				Value: cEntry.CheckpointValue,
			})
		},
		onECEntry: func(ecEntry *pb.ECEntry) {
			if *logEpoch+1 != ecEntry.EpochNumber {
				panic(fmt.Sprintf("dev sanity test: expected epochChange target %d to be exactly one more than our current epoch %d", ecEntry.EpochNumber, *logEpoch))
			}
		},
	})

	return newEpochChange
}
