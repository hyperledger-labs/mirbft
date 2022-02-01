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

package manager

import "github.com/hyperledger-labs/mirbft/request"

// Represents a segment with a non-contiguous ordered range of sequence numbers.
// Each sequence number has a fixed distance form the previous sequence number.
type SkippingSegment struct {
	segID       int
	snDistance  int32
	seqNos      []int32
	leaders     []int32
	followers   []int32
	snOffset    int32 // the first sequence number of the segment
	snLength    int32
	startsAfter int32
	buckets     *request.BucketGroup
	batchSize   int
}

// Initializes the sequence numbers of the segment
// It expects as an argument the order of the segment in case
func (s *SkippingSegment) initSNs() {
	s.seqNos = make([]int32, 0, s.snLength)
	for i := int32(0); i < s.snLength; i++ {
		s.seqNos = append(s.seqNos, s.snOffset+i*s.snDistance)
	}
}

func (s *SkippingSegment) SegID() int {
	return s.segID
}

func (s *SkippingSegment) Leaders() []int32 {
	return s.leaders
}

func (s *SkippingSegment) Followers() []int32 {
	return s.followers
}

// Generates the list of sequence numbers based on offset and length.
func (s *SkippingSegment) SNs() []int32 {
	return s.seqNos
}

func (s *SkippingSegment) FirstSN() int32 {
	return s.snOffset
}

func (s *SkippingSegment) LastSN() int32 {
	return s.SNs()[s.snLength-1]
}

func (s *SkippingSegment) Len() int32 {
	return s.snLength
}

func (s *SkippingSegment) StartsAfter() int32 {
	return s.startsAfter
}

func (s *SkippingSegment) Buckets() *request.BucketGroup {
	return s.buckets
}

func (s *SkippingSegment) BatchSize() int {
	return s.batchSize
}
