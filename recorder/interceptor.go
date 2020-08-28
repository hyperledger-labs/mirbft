/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package recorder

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	pb "github.com/IBM/mirbft/mirbftpb"
	rpb "github.com/IBM/mirbft/recorder/recorderpb"
)

type Interceptor struct {
	timeSource func() int64
	nodeID     uint64
	eventC     chan eventTime
}

type eventTime struct {
	event *pb.StateEvent
	time  int64
}

func NewInterceptor(nodeID uint64, timeSource func() int64, bufferSize int) *Interceptor {
	return &Interceptor{
		nodeID:     nodeID,
		timeSource: timeSource,
		eventC:     make(chan eventTime, bufferSize),
	}
}

func (i *Interceptor) Intercept(event *pb.StateEvent) {
	i.eventC <- eventTime{
		event: event,
		time:  i.timeSource(),
	}
}

func (i *Interceptor) Drain(dest io.Writer, doneC <-chan struct{}) error {
	write := func(eventTime eventTime) error {
		return WriteRecordedEvent(dest, &rpb.RecordedEvent{
			NodeId:     i.nodeID,
			Time:       eventTime.time,
			StateEvent: eventTime.event,
		})
	}

	for {
		select {
		case <-doneC:
			for {
				select {
				case event := <-i.eventC:
					if err := write(event); err != nil {
						return errors.WithMessage(err, "error serializing to stream")
					}
				default:
					return nil
				}
			}
		case event := <-i.eventC:
			if err := write(event); err != nil {
				return errors.WithMessage(err, "error serializing to stream")
			}
		}
	}
}

func WriteRecordedEvent(writer io.Writer, event *rpb.RecordedEvent) error {
	return writeSizePrefixedProto(writer, event)
}

func writeSizePrefixedProto(dest io.Writer, msg proto.Message) error {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithMessage(err, "could not marshal")
	}

	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(lenBuf, int64(len(msgBytes)))
	if _, err = dest.Write(lenBuf[:n]); err != nil {
		return errors.WithMessage(err, "could not write length prefix")
	}

	if _, err = dest.Write(msgBytes); err != nil {
		return errors.WithMessage(err, "could not write message")
	}

	return nil
}

type Reader struct {
	buffer *bytes.Buffer
	source *bufio.Reader
}

func NewReader(source io.Reader) *Reader {
	return &Reader{
		buffer: &bytes.Buffer{},
		source: bufio.NewReader(source),
	}
}

func (r *Reader) ReadEvent() (*rpb.RecordedEvent, error) {
	re := &rpb.RecordedEvent{}
	err := readSizePrefixedProto(r.source, re, r.buffer)
	if err == io.EOF {
		return re, err
	}
	if err != nil {
		return nil, errors.WithMessage(err, "error reading event")
	}
	r.buffer.Reset()

	return re, nil
}

func readSizePrefixedProto(reader *bufio.Reader, msg proto.Message, buffer *bytes.Buffer) error {
	l, err := binary.ReadVarint(reader)
	if err != nil {
		if err == io.EOF {
			return err
		}
		return errors.WithMessage(err, "could not read size prefix")
	}

	buffer.Grow(int(l))

	if _, err := io.CopyN(buffer, reader, l); err != nil {
		return errors.WithMessage(err, "could not read message")
	}

	if err := proto.Unmarshal(buffer.Bytes(), msg); err != nil {
		return errors.WithMessage(err, "could not unmarshal message")
	}

	return nil
}
