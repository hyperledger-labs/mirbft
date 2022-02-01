package manager

import (
	"github.ibm.com/mir-modular/config"
	"github.ibm.com/mir-modular/request"
)

// Represents a segment with a contiguous range of sequence numbers.
// The representation is concise, but accessing the list of sequence
// numbers is expensive, as it is generated each time.
type ContiguousSegment struct {
	segID       int
	leaders     []int32
	followers   []int32
	snOffset    int32
	snLength    int32
	startsAfter int32
	buckets     *request.BucketGroup
}

func (c *ContiguousSegment) SegID() int {
	return c.segID
}

func (c *ContiguousSegment) Leaders() []int32 {
	return c.leaders
}

func (c *ContiguousSegment) Followers() []int32 {
	return c.followers
}

// Generates the list of sequence numbers based on offset and length.
func (c *ContiguousSegment) SNs() []int32 {
	sns := make([]int32, 0, c.snLength)
	for i := c.snOffset; i < c.snOffset+c.snLength; i++ {
		sns = append(sns, i)
	}
	return sns
}

func (c *ContiguousSegment) FirstSN() int32 {
	return c.snOffset
}

func (c *ContiguousSegment) LastSN() int32 {
	return c.snOffset + c.snLength - 1
}

func (c *ContiguousSegment) Len() int32 {
	return c.snLength
}

func (c *ContiguousSegment) StartsAfter() int32 {
	return c.startsAfter
}

func (c *ContiguousSegment) Buckets() *request.BucketGroup {
	return c.buckets
}

func (c *ContiguousSegment) BatchSize() int {
	return config.Config.BatchSize
}
