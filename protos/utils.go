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

package protos

import (
	"github.com/IBM/mirbft/crypto"
	"github.com/golang/protobuf/proto"
)

// Hash returns the hash of the Batch.
func (b *Batch) Hash() []byte {
	return crypto.Hash(b.Header)
}

func (b *Batch) DecodeHeader() *BatchHeader {
	batchheader := &BatchHeader{}
	err := proto.Unmarshal(b.Header, batchheader)
	if err != nil {
		panic(err)
	}

	return batchheader
}

func (b *Batch) IsConfigBatch() bool {
	if b.DecodeHeader().IsConfig != 0 {
		return true
	}
	return false
}
