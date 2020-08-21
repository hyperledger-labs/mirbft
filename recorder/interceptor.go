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

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type Interceptor struct {
	eventC chan *pb.StateEvent
}

func NewInterceptor(bufferSize int) *Interceptor {
	return &Interceptor{
		eventC: make(chan *pb.StateEvent, bufferSize),
	}
}

func (i *Interceptor) Intercept(event *pb.StateEvent) {
	i.eventC <- event
}

func (i *Interceptor) Drain(dest io.Writer, doneC <-chan struct{}) error {
	for {
		select {
		case <-doneC:
			for {
				select {
				case event := <-i.eventC:
					if err := writeSizePrefixedProto(dest, event); err != nil {
						return errors.WithMessage(err, "error serializing to stream")
					}
				default:
					return nil
				}
			}
		case event := <-i.eventC:
			if err := writeSizePrefixedProto(dest, event); err != nil {
				return errors.WithMessage(err, "error serializing to stream")
			}
		}
	}
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

func (r *Reader) ReadEvent() (*pb.StateEvent, error) {
	se := &pb.StateEvent{}
	err := readSizePrefixedProto(r.source, se, r.buffer)
	if err == io.EOF {
		return se, err
	}
	if err != nil {
		return nil, errors.WithMessage(err, "error reading event")
	}
	r.buffer.Reset()

	return se, nil
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
