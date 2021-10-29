/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

func Message(msg *isspb.ISSMessage) *messagepb.Message {
	return &messagepb.Message{Type: &messagepb.Message_Iss{Iss: msg}}
}

func SBMessage(epoch t.EpochNr, instance t.SBInstanceID, msg *isspb.SBInstanceMessage) *messagepb.Message {
	return Message(&isspb.ISSMessage{Type: &isspb.ISSMessage_Sb{Sb: &isspb.SBMessage{
		Epoch:    epoch.Pb(),
		Instance: instance.Pb(),
		Msg:      msg,
	}}})
}
