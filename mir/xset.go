/*
Copyright IBM Corp. 2021 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mir

import (
	"reflect"

	pb "github.com/IBM/mirbft/protos"
)

// makeXset returns a request subject that should be proposed as batches
// for new-view.  If there is no request to select (null request), it
// will return nil for subject.  makeXset always returns a batches for
// the most recent checkpoint.
func (s *SBFT) makeXset(vcs []*pb.ViewChange) ([]*pb.Subject, *pb.Batch, bool) {
	// first select base commit (equivalent to checkpoint/low water mark)
	var best *pb.Batch
	last := uint64(0) // the highest seq number in Q
	for _, vc := range vcs {
		seq := vc.Checkpoint.DecodeHeader().Seq
		if best == nil || seq > best.DecodeHeader().Seq {
			best = vc.Checkpoint
		}
		for _, entry := range vc.Pset {
			if entry.Seq.Seq > last {
				last = entry.Seq.Seq
			}
		}
		for _, entry := range vc.Qset {
			if entry.Seq.Seq > last {
				last = entry.Seq.Seq
			}
		}
	}

	if best == nil {
		return nil, nil, false
	}

	first := best.DecodeHeader().Seq + 1
	xset := make([]*pb.Subject, 0, 0)
	log.Debugf("replica %d: xset starts at commit %d", s.id, first)
	log.Debugf("replica %d: xset stops at commit %d", s.id, last)

	// now determine which batches could have executed for best+1
	for next := first; next <= last; next++ {
		xsetEntry := &pb.Subject{
			Seq: &pb.SeqView{Seq: next, View: s.view},
		}
		// This is according to Castro's TOCS PBFT, Fig. 4
		// find some message m in S,
		emptycount := 0
	nextm:
		for _, m := range vcs {
			notfound := true
			// which has <n,d,v> in its Pset
			for _, mtuple := range m.Pset {
				log.Debugf("replica %d: trying %v", s.id, mtuple)
				if mtuple.Seq.Seq != next {
					continue
				}

				// we found an entry for next
				notfound = false

				// A1. where 2f+1 messages mp from S
				count := 0
			nextmp:
				for _, mp := range vcs {
					// "low watermark" is less than n
					if mp.Checkpoint.DecodeHeader().Seq > mtuple.Seq.Seq {
						continue
					}
					// and all <n,d',v'> in its Pset
					for _, mptuple := range mp.Pset {
						log.Debugf("replica %d: matching %v", s.id, mptuple)
						if mptuple.Seq.Seq != mtuple.Seq.Seq {
							continue
						}

						// either v' < v or (v' == v and d' == d)
						if mptuple.Seq.View < mtuple.Seq.View ||
							(mptuple.Seq.View == mtuple.Seq.View && reflect.DeepEqual(mptuple.Digest, mtuple.Digest)) {
							continue
						} else {
							continue nextmp
						}
					}
					count += 1
				}
				if count < s.viewChangeQuorum() {
					continue
				}
				log.Debugf("replica %d: found %d replicas for Pset %d/%d", s.id, count, mtuple.Seq.Seq, mtuple.Seq.View)

				// A2. f+1 messages mp from S
				count = 0
				for _, mp := range vcs {
					// and all <n,d',v'> in its Qset
					for _, mptuple := range mp.Qset {
						if mptuple.Seq.Seq != mtuple.Seq.Seq {
							continue
						}
						if mptuple.Seq.View < mtuple.Seq.View {
							continue
						}
						// d' == d
						if !reflect.DeepEqual(mptuple.Digest, mtuple.Digest) {
							continue
						}
						count += 1
						// there exists one ...
						break
					}
				}
				if count < s.oneCorrectQuorum() {
					continue
				}
				log.Debugf("replica %d: found %d replicas for Qset %d", s.id, count, mtuple.Seq.Seq)
				log.Debugf("replica %d: selecting %d with %x", s.id, next, mtuple.Digest)
				xsetEntry.Digest = mtuple.Digest
				xset = append(xset, xsetEntry)
				break nextm
			}

			if notfound {
				emptycount += 1
			}
		}

		// B. otherwise select null request
		if emptycount >= s.viewChangeQuorum() {
			log.Debugf("replica %d: no pertinent requests found for %d", s.id, next)
			last = next - 1
			break
			// Stop populating the xset when first null is found
		}
	}

	log.Debugf("replica %d: xset lenght %d", s.id, len(xset))
	if uint64(len(xset)) != last-first+1 {
		return nil, nil, false
	}

	return xset, best, true
}
