package manager

import "github.ibm.com/mir-modular/request"

// Represents a Segment of the log that is handled by one ordering instance, independently of and in parallel with
// other Segments. Segments are created by the Manager and all nodes receive information about Segments they are
// involved in.
type Segment interface {
	// Globally unique ID of the Segment
	SegID() int

	// The sequence of leaders for this Segment.
	// In case the leader changes len(Leaders) times, we expect the ordering instance to wrap around.
	Leaders() []int32

	// List of followers involved in ordering this Segment.
	Followers() []int32

	// Ordered list of sequence numbers this Segment consists of.
	SNs() []int32

	// Lowest sequence number that
	//is part of the Segment.
	FirstSN() int32

	// Highest sequence number that is part of the Segment.
	LastSN() int32

	// Number of sequence numbers in this Segment.
	Len() int32

	// Sequence number for which an entry has to be committed before
	// the leader of this segment can make any propositions.
	// Used for duplication prevention - so that multiple segments do not propose values from the same bucket.
	// A value of -1 means that the segment can start immediately.
	StartsAfter() int32

	// Returns a group of request buckets.
	// The leader of this segment uses exclusively these buckets to obtain batches of requests to propose.
	Buckets() *request.BucketGroup

	// Batch size limit for this segment.
	BatchSize() int
}
