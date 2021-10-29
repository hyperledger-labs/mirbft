/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspbftpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

func PbftPreprepareMessage(sn t.SeqNr, batch *requestpb.Batch) *isspb.SBInstanceMessage {
	return &isspb.SBInstanceMessage{Type: &isspb.SBInstanceMessage_PbftPreprepare{
		PbftPreprepare: &isspbftpb.Preprepare{
			Sn:    sn.Pb(),
			Batch: batch,
		},
	}}
}
