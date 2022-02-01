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

package log

import pb "github.com/hyperledger-labs/mirbft/protobufs"

// TODO: Consider making this a protobuf message, as it may need to be contained in the missing entry response.
type Entry struct {
	Sn        int32
	Batch     *pb.Batch
	Digest    []byte
	Aborted   bool
	Suspect   int32 // If aborted then this is the first leader of the segment
	ProposeTs int64
	CommitTs  int64
}
