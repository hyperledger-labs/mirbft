/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protos

//go:generate protoc --proto_path=. --go_out=../pkg/pb/ --go_opt=paths=source_relative messagepb/messagepb.proto
//go:generate protoc --proto_path=. --go_out=../pkg/pb/ --go_opt=paths=source_relative requestpb/requestpb.proto
//go:generate protoc --proto_path=. --go_out=../pkg/pb/ --go_opt=paths=source_relative eventpb/eventpb.proto
//go:generate protoc --proto_path=. --go_out=../pkg/pb/ --go_opt=paths=source_relative statuspb/statuspb.proto
//go:generate protoc --proto_path=. --go_out=../pkg/pb/ --go_opt=paths=source_relative recordingpb/recordingpb.proto
//go:generate protoc --proto_path=. --go_out=../pkg/pb/ --go_opt=paths=source_relative isspb/isspb.proto
//go:generate protoc --proto_path=. --go_out=../pkg/pb/ --go_opt=paths=source_relative isspbftpb/isspbftpb.proto

//go:generate protoc --proto_path=. --go_out=:../pkg/ --go_opt=paths=source_relative simplewal/simplewal.proto
//go:generate protoc --proto_path=. --go_out=:../samples/ --go_opt=paths=source_relative chat-demo/chatdemo.proto
//go:generate protoc --go_out=../pkg/ --go_opt=paths=source_relative --go-grpc_out=../pkg/ --go-grpc_opt=paths=source_relative requestreceiver/requestreceiver.proto
//go:generate protoc --go_out=../pkg/ --go_opt=paths=source_relative --go-grpc_out=../pkg/ --go-grpc_opt=paths=source_relative grpctransport/grpctransport.proto
//xgo:generate protoc --proto_path=. --go_out=plugins=grpc:../pkg/ --go_opt=paths=source_relative grpctransport/grpctransport.proto
//xgo:generate protoc --proto_path=. --go_out=plugins=grpc:../pkg/ --go_opt=paths=source_relative requestreceiver/requestreceiver.proto
