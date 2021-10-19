package iss

import t "github.com/hyperledger-labs/mirbft/pkg/types"

type LeaderSelectionPolicy interface {
	Leaders(e t.EpochNr) []t.NodeID
	Suspect(e t.EpochNr, node t.NodeID)
}
