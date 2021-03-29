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
	"math/big"

	"github.com/IBM/mirbft/config"
)

func (s *SBFT) getsPayload(id uint64, digest []byte) bool {
	keyInt := new(big.Int)
	keyInt.SetBytes(digest)
	for _, b := range s.pBuckets[id] {
		if keyInt.Cmp(b[0]) == 1 && keyInt.Cmp(b[1]) != 1 {
			return true
		}
	}
	return false
}

func (s *SBFT) verifies(id uint64, digest []byte) bool {
	keyInt := new(big.Int)
	keyInt.SetBytes(digest)
	for _, b := range s.vBuckets[id] {
		if keyInt.Cmp(b[0]) == 1 && keyInt.Cmp(b[1]) != 1 {
			return true
		}
	}
	return false
}

func (s *SBFT) assignVerificationBuckets() {
	L := new(big.Int)
	L.SetString(hspace, 10)
	one := big.NewInt(1)
	if config.Config.SigSharding {
		verifiers := big.NewInt(int64(config.Config.N))
		bin := new(big.Int)
		bin.Div(L, verifiers)
		fPlusOne := big.NewInt(int64(config.Config.F))
		fPlusOne.Add(fPlusOne, one)
		for i := int64(0); i < int64(config.Config.N); i++ {
			s.vBuckets[uint64(i)] = make([][]*big.Int, 1)
			s.vBuckets[uint64(i)][0] = make([]*big.Int, 2)
			s.vBuckets[uint64(i)][0][0] = new(big.Int)
			s.vBuckets[uint64(i)][0][0].Mul(big.NewInt(int64(i-int64(config.Config.F))), bin)
			s.vBuckets[uint64(i)][0][1] = new(big.Int)
			s.vBuckets[uint64(i)][0][1].Mul(fPlusOne, bin)
			s.vBuckets[uint64(i)][0][1].Add(s.vBuckets[uint64(i)][0][0], s.vBuckets[uint64(i)][0][1])
			s.vBuckets[uint64(i)][0][0].Mod(s.vBuckets[uint64(i)][0][0], L)
			s.vBuckets[uint64(i)][0][1].Mod(s.vBuckets[uint64(i)][0][1], L)
		}
		for i, bucket := range s.vBuckets {
			if bucket[0][0].Cmp(bucket[0][1]) != -1 {
				temp := make([]*big.Int, 2)
				temp[0] = big.NewInt(0)
				temp[0].Add(temp[0], bucket[0][0])
				temp[1] = big.NewInt(0)
				temp[1].Add(temp[1], L)
				bucket[0][0] = big.NewInt(0)
				s.vBuckets[i] = append(s.vBuckets[i], temp)
			}
		}

	} else {
		for i := uint64(0); i < uint64(config.Config.N); i++ {
			s.vBuckets[i] = make([][]*big.Int, 1)
			s.vBuckets[i][0] = make([]*big.Int, 2)
			s.vBuckets[i][0][0] = big.NewInt(0)
			s.vBuckets[i][0][1] = new(big.Int).Mul(one, L)
		}
	}

	if len(s.vBuckets[s.id]) > 1 {
		log.Errorf("replica %d: Verifies batches in [%x, %x) and [%x,%x)", s.id, s.vBuckets[s.id][0][0], s.vBuckets[s.id][0][1], s.vBuckets[s.id][1][0], s.vBuckets[s.id][1][1])
	} else {
		log.Errorf("replica %d: Verifies batches in [%x, %x)", s.id, s.vBuckets[s.id][0][0], s.vBuckets[s.id][0][1])
	}
}

func (s *SBFT) assignPayloadBuckets(inRecovery bool) {
	L := new(big.Int)
	L.SetString(hspace, 10)
	one := big.NewInt(1)
	two := big.NewInt(2)
	if config.Config.PayloadSharding {
		verifiers := big.NewInt(int64(config.Config.N))
		bin := new(big.Int)
		bin.Div(L, verifiers)
		quorum := big.NewInt(int64(config.Config.F))
		if inRecovery {
			quorum.Mul(quorum, two)
		}
		quorum.Add(quorum, one)
		for i := int64(0); i < int64(config.Config.N); i++ {
			s.pBuckets[uint64(i)] = make([][]*big.Int, 1)
			s.pBuckets[uint64(i)][0] = make([]*big.Int, 2)
			s.pBuckets[uint64(i)][0][0] = new(big.Int)
			s.pBuckets[uint64(i)][0][0].Mul(big.NewInt(int64(i-int64(config.Config.F))), bin)
			s.pBuckets[uint64(i)][0][1] = new(big.Int)
			s.pBuckets[uint64(i)][0][1].Mul(quorum, bin)
			s.pBuckets[uint64(i)][0][1].Add(s.pBuckets[uint64(i)][0][0], s.pBuckets[uint64(i)][0][1])
			s.pBuckets[uint64(i)][0][0].Mod(s.pBuckets[uint64(i)][0][0], L)
			s.pBuckets[uint64(i)][0][1].Mod(s.pBuckets[uint64(i)][0][1], L)
		}
		for i, bucket := range s.pBuckets {
			if bucket[0][0].Cmp(bucket[0][1]) != -1 {
				temp := make([]*big.Int, 2)
				temp[0] = big.NewInt(0)
				temp[0].Add(temp[0], bucket[0][0])
				temp[1] = big.NewInt(0)
				temp[1].Add(temp[1], L)
				bucket[0][0] = big.NewInt(0)
				s.pBuckets[i] = append(s.pBuckets[i], temp)
			}
		}

	} else {
		for i := uint64(0); i < uint64(config.Config.N); i++ {
			s.pBuckets[i] = make([][]*big.Int, 1)
			s.pBuckets[i][0] = make([]*big.Int, 2)
			s.pBuckets[i][0][0] = big.NewInt(0)
			s.pBuckets[i][0][1] = new(big.Int).Mul(one, L)
		}
	}

	if len(s.pBuckets[s.id]) > 1 {
		log.Errorf("replica %d: Payload for batches in [%x, %x) and [%x,%x)", s.id, s.pBuckets[s.id][0][0], s.pBuckets[s.id][0][1], s.pBuckets[s.id][1][0], s.pBuckets[s.id][1][1])
	} else {
		log.Errorf("replica %d: Payload for batches in [%x, %x)", s.id, s.pBuckets[s.id][0][0], s.pBuckets[s.id][0][1])
	}
}
