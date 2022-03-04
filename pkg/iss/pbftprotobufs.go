/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// This file provides constructors for protobuf messages (also used to represent events) used by the ISS PBFT orderer.
// The primary purpose is convenience and improved readability of the PBFT code,
// As creating protobuf objects is rather verbose in Go.
// Moreover, in case the definitions of some protocol buffers change,
// this file should be the only one that will potentially need to change.
// TODO: When PBFT is moved to a different package, remove the Pbft prefix form the function names defined in this file.

// TODO: Write documentation comments for the functions in this file.
//       Part of the text can probably be copy-pasted from the documentation of the functions handling those events.

package iss

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspbftpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// ============================================================
// Events
// ============================================================

func PbftPersistPreprepare(preprepare *isspbftpb.Preprepare) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_PbftPersistPreprepare{
		PbftPersistPreprepare: &isspbftpb.PersistPreprepare{
			Preprepare: preprepare,
		},
	}}
}

func PbftPersistPrepare(prepare *isspbftpb.Prepare) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_PbftPersistPrepare{
		PbftPersistPrepare: &isspbftpb.PersistPrepare{
			Prepare: prepare,
		},
	}}
}

func PbftPersistCommit(commit *isspbftpb.Commit) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_PbftPersistCommit{
		PbftPersistCommit: &isspbftpb.PersistCommit{
			Commit: commit,
		},
	}}
}

// TODO: Generalize the Preprepare, Prepare, and Commit persist events to one (PersistMessage)

func PbftReqWaitReference(sn t.SeqNr, view t.PBFTViewNr) *isspb.SBReqWaitReference {
	return &isspb.SBReqWaitReference{Type: &isspb.SBReqWaitReference_Pbft{Pbft: &isspbftpb.ReqWaitReference{
		Sn:   sn.Pb(),
		View: view.Pb(),
	}}}
}

// ============================================================
// Messages
// ============================================================

func PbftPreprepareMessage(content *isspbftpb.Preprepare) *isspb.SBInstanceMessage {
	return &isspb.SBInstanceMessage{Type: &isspb.SBInstanceMessage_PbftPreprepare{
		PbftPreprepare: content,
	}}
}

func PbftPrepareMessage(content *isspbftpb.Prepare) *isspb.SBInstanceMessage {
	return &isspb.SBInstanceMessage{Type: &isspb.SBInstanceMessage_PbftPrepare{
		PbftPrepare: content,
	}}
}

func PbftCommitMessage(content *isspbftpb.Commit) *isspb.SBInstanceMessage {
	return &isspb.SBInstanceMessage{Type: &isspb.SBInstanceMessage_PbftCommit{
		PbftCommit: content,
	}}
}
