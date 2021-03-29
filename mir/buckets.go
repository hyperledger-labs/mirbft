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
	"encoding/binary"
	"math/big"
	"sync/atomic"

	"github.com/IBM/mirbft/config"
	pb "github.com/IBM/mirbft/protos"
)

func (s *SBFT) isRotationPrimary(id uint64) bool {
	if s.isInRecovery() {
		return s.primaryID() == id
	}
	return id == s.rotationPrimary
}

// Returns the bucket the digest of a request maps to
func (s *SBFT) getBucket(digest []byte) uint64 {

	if s.bucketSize == nil {
		panic("Bucket size not set")
	}
	keyInt := new(big.Int)
	keyInt.SetBytes(digest)

	bucket := new(big.Int).Div(keyInt, s.bucketSize)

	i := bucket.Uint64()

	if i >= uint64(len(s.buckets)) {
		panic("getBucket -- going beyond bucket limits")
	}

	b := s.buckets[i]

	if keyInt.Cmp(b[0]) != 1 || keyInt.Cmp(b[1]) == 1 {
		panic("Wrong result in getBucket")
	}

	return i
}

func (s *SBFT) inActiveBucket(id uint64, seq uint64, digest []byte) bool {
	buckets := s.activeBuckets

	keyInt := new(big.Int)
	keyInt.SetBytes(digest)
	last := len(buckets[id]) - 1
	if buckets[id][0] < buckets[id][last] {
		if keyInt.Cmp(s.buckets[buckets[id][0]][0]) == 1 && keyInt.Cmp(s.buckets[buckets[id][last]][1]) != 1 {
			return true
		}
	} else {
		if (keyInt.Cmp(s.buckets[buckets[id][0]][0]) == 1 && keyInt.Cmp(s.buckets[uint64(len(s.buckets)-1)][1]) != 1) ||
			(keyInt.Cmp(big.NewInt(0)) == 1 && keyInt.Cmp(s.buckets[buckets[id][last]][1]) != 1) {
			return true
		}
	}
	return false
}

func (s *SBFT) rotates(seq uint64) bool {
	next := s.lastRotation + uint64(config.Config.BucketRotationPeriod)
	return next <= seq
}

// returns how many rotations ahead from the current rotation seq is
func (s *SBFT) rotationDistance(seq uint64) uint64 {
	return (seq - s.lastRotation) / uint64(config.Config.BucketRotationPeriod)
}

func (s *SBFT) rotateBuckets() {
	log.Debugf("replica %d: HERE rotating buckets", s.id)
	// 1. if only one leader no possible rotation
	if len(s.leaders) < 2 {
		return
	}
	// 1. if in stable mode advance the rotation primary in round robin way
	if !s.isInRecovery() {
		//s.rotationPrimary = (s.rotationPrimary + 1) % s.config.N
		s.rotationPrimary = (s.rotationPrimary + 1) % uint64(len(s.leaders)) //TODO is this correct?
	}
	// 2. reassign the buckets
	s.assignBuckets()
	lr := s.lastRotation + uint64(config.Config.BucketRotationPeriod)
	atomic.StoreUint64(&s.lastRotation, lr)
}

func (s *SBFT) assignBuckets() {
	s.activeBuckets, s.nextActiveBucket = s.getBuckets(1)
	s.bucketToLeaderLock.Lock()
	for leader, vector := range s.activeBuckets {
		for _, b := range vector {
			s.bucketToLeader[b] = leader
		}
	}
	s.bucketToLeaderLock.Unlock()
	if s.isLeader(s.id) {
		log.Criticalf("replica %d: Responsible for requests in buckets %+v in epoch %d", s.id, s.activeBuckets[s.id], s.view)
		log.Debugf("replica %d: buckets to leaders %+v", s.id, s.bucketToLeader)
		s.lastActiveBucket = s.activeBuckets[s.id][len(s.activeBuckets[s.id])-1]
	}
}

// getBuckets returns the active buckets and the next active bucket
// dist is the number of rotations
func (s *SBFT) getBuckets(dist uint64) (map[uint64][]uint64, map[uint64]uint64) {
	activeBuckets := make(map[uint64][]uint64)
	next := make(map[uint64]uint64)

	for rotation := uint64(0); rotation < dist; rotation++ {
		// 1. find the index of the rotation primary
		rotationPrimaryIndex := uint64(0)
		rotationPrimary := uint64(0) // either rotation primary or epoch leader
		for i, r := range s.leaders {
			if s.isRotationPrimary(s.leaders[i]) {
				rotationPrimaryIndex = i
				rotationPrimary = r
				break
			}
		}
		log.Errorf("replica %d: Rotation primary is %d:%d", s.id, rotationPrimaryIndex, rotationPrimary)
		// 2. create a shifted-leaders table starting from the rotation primary
		shiftedLeaders := make(map[uint64]uint64)
		for i, _ := range s.leaders {
			shiftedLeaders[uint64(mod(int64(i)-int64(rotationPrimaryIndex), int64(len(s.leaders))))] = s.leaders[i]
		}
		log.Errorf("replica %d: Shifted leaders %+v", s.id, shiftedLeaders)
		// 3. define as offset the starting point for the rotation primary
		offset := s.nextActiveBucket[rotationPrimary]
		log.Errorf("replica %d: Offset %+d", s.id, offset)
		// 4. assign the buckets accordingly starting from the offset
		totalBuckets := uint64(config.Config.MaxLeaders * config.Config.Buckets)
		bucketsPerLeader := uint64(totalBuckets / uint64(len(s.leaders)))
		log.Errorf("replica %d: Buckets per leader %d", s.id, bucketsPerLeader)
		remainder := totalBuckets - bucketsPerLeader*uint64(len(s.leaders))
		log.Errorf("replica %d: Remaining buckets %d", s.id, remainder)
		move := uint64(0)
		for i := uint64(0); i < uint64(len(shiftedLeaders)); i++ {
			leader := shiftedLeaders[i]
			activeBuckets[leader] = make([]uint64, 0, 0)
			activeBuckets[leader] = append(activeBuckets[leader], (move+offset+bucketsPerLeader*i)%totalBuckets)
			last := activeBuckets[leader][0] + bucketsPerLeader - 1
			for j := activeBuckets[leader][0] + 1; j <= last; j++ {
				activeBuckets[leader] = append(activeBuckets[leader], j%totalBuckets)
			}
			if remainder > 0 {
				activeBuckets[leader] = append(activeBuckets[leader], (last+1)%totalBuckets)
				remainder--
				move++
			}
		}

		// 5. update highest covered value per leader
		for _, leader := range s.leaders {
			next[leader] = (activeBuckets[leader][len(activeBuckets[leader])-1] + 1) % totalBuckets
		}
	}
	return activeBuckets, next
}

func (s *SBFT) checkForBucketConfig(pp *pb.Preprepare) {
	if s.view > 0 {
		header := pp.Batch.DecodeHeader()
		if header.IsConfig == 2 {
			s.nextActiveBucketLock.Lock()
			s.nextActiveBucket[pp.Leader] = binary.BigEndian.Uint64(pp.Batch.Payloads[len(pp.Batch.Payloads)-1])
			s.nextActiveBucketLock.Unlock()
		}
	}
}

// return the next active bucket base on the bucket priority queue
func (s *SBFT) getNextActiveBucket() uint64 {
	// if the priority list is empty move to the next consequent
	s.bucketPriorityLock.Lock()
	if len(s.bucketPriority) == 0 {
		nextActiveBucket := (s.lastActiveBucket + 1) % uint64(config.Config.MaxLeaders*config.Config.Buckets)
		s.bucketPriorityLock.Unlock()
		return nextActiveBucket
	}

	next := 0
	for i := 0; i < len(s.bucketPriority); i++ {
		index := s.bucketPriority[uint64(i)]
		s.bucketLocks[index].RLock()
		if len(s.pending[index]) > 0 { //TODO lock s.pending; also -- should it not be s.pending(s.bucketPriority[i])
			next = i
			s.bucketLocks[index].RUnlock()
			break
		}
		s.bucketLocks[index].RUnlock()
	}
	nextActiveBucket := s.bucketPriority[next]
	s.bucketPriority = s.bucketPriority[next+1:]

	s.bucketLocks[nextActiveBucket].RLock()
	l := len(s.pending[nextActiveBucket])
	s.bucketLocks[nextActiveBucket].RUnlock()

	if l > 0 {
		s.bucketPriority = append(s.bucketPriority, nextActiveBucket)
	}
	s.bucketPriorityLock.Unlock()
	return nextActiveBucket
}

// Sets the "buckets" map in SBFT instance. Mapping from bucketID -> [range of hash space]
// Example: With 4 leaders and 4 buckets in config, Total number of buckets = 4 * 4 = 16
// s.buckets = {0:[start_index hash space, end_index hash space], 1:[], ... ,15:[]}
func (s *SBFT) initBuckets() {
	s.buckets = make(map[uint64][]*big.Int)
	L := new(big.Int)
	L.SetString(hspace, 10)

	leaders := big.NewInt(int64(config.Config.MaxLeaders))
	log.Errorf("s.config.Buckets is %d", config.Config.Buckets)
	size := new(big.Int).Div(L, leaders.Mul(leaders, big.NewInt(int64(config.Config.Buckets))))
	s.bucketSize = size
	bucketNum := uint64(config.Config.MaxLeaders * config.Config.Buckets)
	for i := uint64(0); i < bucketNum; i++ {
		s.buckets[i] = make([]*big.Int, 2, 2)
		s.buckets[i][0] = new(big.Int).Mul(big.NewInt(int64(i)), size)
		s.buckets[i][1] = new(big.Int).Add(s.buckets[i][0], size)
	}
	s.buckets[bucketNum-uint64(1)][1] = L
}

func (s *SBFT) getLeaderPendingSize() int64 {
	return atomic.LoadInt64(&s.leaderPendingSize)
}

func (s *SBFT) addToLeaderPendingSize(sz int64) {
	atomic.AddInt64(&s.leaderPendingSize, sz)
}

func (s *SBFT) subtractFromLeaderPendingSize(sz int64) {
	atomic.AddInt64(&s.leaderPendingSize, -sz)
}

//FIXME for efficiency of this method, we are currently just assuming all requests have the same size
func (s *SBFT) updateLeaderPendingSize() {
	var lps int64 = 0
	atomic.StoreInt64(&s.leaderPendingSize, int64(0))
	for _, bucket := range s.activeBuckets[s.id] {
		s.bucketLocks[bucket].RLock()
		if len(s.pending[bucket]) != 0 {
			var sz int64 = 0
			for _, req := range s.pending[bucket] {
				sz = int64(reqSize(req.request))
				break
			}
			lps += sz * int64(len(s.pending[bucket]))
		}
		s.bucketLocks[bucket].RUnlock()
	}
	atomic.StoreInt64(&s.leaderPendingSize, lps)
}
