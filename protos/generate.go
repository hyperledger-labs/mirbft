/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protos

//go:generate protoc --proto_path=. --go_out=../pkg/pb/ --go_opt=paths=source_relative msgs/msgs.proto state/state.proto recording/recording.proto
