/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package recorder

import (
	"bufio"
	"bytes"
	"compress/gzip"
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
	doneC      chan struct{}
	exitC      chan struct{}
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
		doneC:      make(chan struct{}),
		exitC:      make(chan struct{}),
	}
}

func (i *Interceptor) Intercept(event *pb.StateEvent) {
	select {
	case i.eventC <- eventTime{
		event: event,
		time:  i.timeSource(),
	}:
	case <-i.doneC:
	}
}

func (i *Interceptor) Stop() {
	close(i.doneC)
	<-i.exitC
}

func (i *Interceptor) Drain(dest io.Writer) error {
	defer close(i.exitC)

	gzWriter := gzip.NewWriter(dest)
	defer gzWriter.Close()

	write := func(eventTime eventTime) error {
		return WriteRecordedEvent(gzWriter, &rpb.RecordedEvent{
			NodeId:     i.nodeID,
			Time:       eventTime.time,
			StateEvent: eventTime.event,
		})
	}

	for {
		select {
		case <-i.doneC:
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
	buffer   *bytes.Buffer
	gzReader *gzip.Reader
	source   *bufio.Reader
}

func NewReader(source io.Reader) (*Reader, error) {
	gzReader, err := gzip.NewReader(source)
	if err != nil {
		return nil, errors.WithMessage(err, "could not read source as a gzip stream")
	}

	return &Reader{
		buffer:   &bytes.Buffer{},
		gzReader: gzReader,
		source:   bufio.NewReader(gzReader),
	}, nil
}

func (r *Reader) ReadEvent() (*rpb.RecordedEvent, error) {
	re := &rpb.RecordedEvent{}
	err := readSizePrefixedProto(r.source, re, r.buffer)
	if err == io.EOF {
		r.gzReader.Close()
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
