/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	pref "google.golang.org/protobuf/reflect/protoreflect"
)

// this is a custom text marshaler for mirbft proto messages
// it is effectively a small fork of
// https://github.com/protocolbuffers/protobuf-go/blob/v1.25.0/encoding/prototext/encode.go

func textFormat(msg proto.Message, shortBytes bool) (string, error) {
	te := &textEncoder{
		shortBytes: shortBytes,
		result:     &bytes.Buffer{},
	}
	err := te.marshalMessage(msg.ProtoReflect())
	if err != nil {
		return "", err
	}

	return te.result.String(), nil
}

type textEncoder struct {
	shortBytes bool
	result     *bytes.Buffer
}

func (te *textEncoder) writeName(name string) {
	l := te.result.Len()
	if l > 0 && te.result.String()[l-1] != '[' {
		te.result.WriteString(" ")
	}
	te.result.WriteString(name)
	te.result.WriteString("=")
}

func (te *textEncoder) marshalMessage(m pref.Message) error {
	messageDesc := m.Descriptor()
	fieldDescs := messageDesc.Fields()
	size := fieldDescs.Len()
	te.result.WriteString("[")
	for i := 0; i < size; {
		fd := fieldDescs.Get(i)
		if od := fd.ContainingOneof(); od != nil {
			fd = m.WhichOneof(od)
			i += od.Fields().Len()
		} else {
			i++
		}

		name := fd.Name()
		// Use type name for group field name.
		if fd.Kind() == pref.GroupKind {
			name = fd.Message().Name()
		}

		val := m.Get(fd)
		if err := te.marshalField(string(name), val, fd); err != nil {
			return err
		}
	}
	te.result.WriteString("]")

	return nil
}

// marshalField marshals the given field with protoreflect.Value.
func (te *textEncoder) marshalField(name string, val pref.Value, fd pref.FieldDescriptor) error {
	switch {
	case fd.IsList():
		return te.marshalList(name, val.List(), fd)
	case fd.IsMap():
		return errors.Errorf("maps in protos unsupported by this encoder")
	default:
		te.writeName(name)
		return te.marshalSingular(val, fd)
	}
}

// marshalSingular marshals the given non-repeated field value. This includes
// all scalar types, enums, messages, and groups.
func (te *textEncoder) marshalSingular(val pref.Value, fd pref.FieldDescriptor) error {
	kind := fd.Kind()
	switch kind {
	case pref.BoolKind:
		fmt.Fprintf(te.result, "%t", val.Bool())
	case pref.StringKind:
		s := val.String()
		te.result.WriteString(s)
	case pref.Int32Kind, pref.Int64Kind,
		pref.Sint32Kind, pref.Sint64Kind,
		pref.Sfixed32Kind, pref.Sfixed64Kind:
		fmt.Fprintf(te.result, "%d", val.Int())

	case pref.Uint32Kind, pref.Uint64Kind,
		pref.Fixed32Kind, pref.Fixed64Kind:
		fmt.Fprintf(te.result, "%d", val.Uint())
	case pref.BytesKind:
		origVal := val.Bytes()
		var val []byte
		if te.shortBytes && len(origVal) > 4 {
			val = origVal[:4]
		} else {
			val = origVal
		}
		fmt.Fprintf(te.result, "%x", val)
	case pref.EnumKind:
		num := val.Enum()
		if desc := fd.Enum().Values().ByNumber(num); desc != nil {
			te.result.WriteString(string(desc.Name()))
		} else {
			// Use numeric value if there is no enum description.
			fmt.Fprintf(te.result, "%d", num)
		}
	case pref.MessageKind, pref.GroupKind:
		return te.marshalMessage(val.Message())
	default:
		panic(fmt.Sprintf("%v has unsupported field kind: %v", fd.FullName(), kind))
	}
	return nil
}

// marshalList marshals the given protoreflect.List as multiple name-value fields.
func (te *textEncoder) marshalList(name string, list pref.List, fd pref.FieldDescriptor) error {
	size := list.Len()
	for i := 0; i < size; i++ {
		te.writeName(name)
		if err := te.marshalSingular(list.Get(i), fd); err != nil {
			return err
		}
	}
	return nil
}
