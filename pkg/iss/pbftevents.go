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

func PbftPersistPreprepare(sn t.SeqNr, batch *requestpb.Batch) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_PbftPersistPreprepare{
		PbftPersistPreprepare: &isspbftpb.PersistPreprepare{
			Preprepare: &isspbftpb.Preprepare{
				Sn:    sn.Pb(),
				Batch: batch,
			},
		},
	}}
}
