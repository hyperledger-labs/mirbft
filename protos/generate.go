/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protos

//go:generate protoc --proto_path=. --go_out=../pkg/pb/ --go_opt=paths=source_relative eventpb/eventpb.proto messagepb/messagepb.proto recordingpb/recordingpb.proto
