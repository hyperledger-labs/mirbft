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
	pb "github.com/IBM/mirbft/protos"
)

// Connection is an event from system to notify a new connection with
// replica.
// On connection, we send our latest (weak) checkpoint, and we expect
// to receive one from replica.
func (s *SBFT) Connection(replica uint64) {
	// TODO
	// 1: send the latest checkpoint
	// 2a: if epoch-primary send the latest epoch configuration you applied
	// 2b: if non-epoch primary send the latest subject for epoch config you applied
	// c: replay all the messages you have sent above the latest checkpoint
}

func (s *SBFT) handleHello(h *pb.Hello, src uint64) {
	// TODO
	// 1: check the latest checkpoint - if you are above the checkpoint you did not miss anything important
	// 2: check if you are above the latest epoch configuration and apply the epoch config if you collect at least f+1 a
}
