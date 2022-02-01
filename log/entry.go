package log

import pb "github.ibm.com/mir-modular/protobufs"

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
