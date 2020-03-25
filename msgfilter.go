/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	pb "github.com/IBM/mirbft/mirbftpb"

	"github.com/pkg/errors"
)

// TODO, we probably can/should add some more basic error checking here.  Particularly
// identifying pointer fields which must be set.

func preProcess(outerMsg *pb.Msg) error {
	switch innerMsg := outerMsg.Type.(type) {
	case *pb.Msg_Preprepare:
		if innerMsg.Preprepare == nil {
			return errors.Errorf("message of type Preprepare, but preprepare field is nil")
		}
	case *pb.Msg_Prepare:
		if innerMsg.Prepare == nil {
			return errors.Errorf("message of type Prepare, but prepare field is nil")
		}
	case *pb.Msg_Commit:
		if innerMsg.Commit == nil {
			return errors.Errorf("message of type Commit, but commit field is nil")
		}
	case *pb.Msg_Suspect:
		if innerMsg.Suspect == nil {
			return errors.Errorf("message of type Suspect, but suspect field is nil")
		}
	case *pb.Msg_Checkpoint:
		if innerMsg.Checkpoint == nil {
			return errors.Errorf("message of type Checkpoint, but checkpoint field is nil")
		}
	case *pb.Msg_Forward:
		if innerMsg.Forward == nil {
			return errors.Errorf("message of type Forward, but forward field is nil")
		}
	case *pb.Msg_EpochChange:
		if innerMsg.EpochChange == nil {
			return errors.Errorf("message of type EpochChange, but epoch_change field is nil")
		}
	case *pb.Msg_NewEpoch:
		switch {
		case innerMsg.NewEpoch == nil:
			return errors.Errorf("message of type NewEpoch, but new_epoch field is nil")
		case innerMsg.NewEpoch.Config == nil:
			return errors.Errorf("NewEpoch has nil Config")
		case innerMsg.NewEpoch.Config.StartingCheckpoint == nil:
			return errors.Errorf("NewEpoch Config has nil StartingCheckpoint")
		}
	case *pb.Msg_NewEpochEcho:
		switch {
		case innerMsg.NewEpochEcho == nil:
			return errors.Errorf("message of type NewEpochEcho, but new_epoch_echo field is nil")
		case innerMsg.NewEpochEcho.Config == nil:
			return errors.Errorf("NewEpochEcho has nil Config")
		}
	case *pb.Msg_NewEpochReady:
		switch {
		case innerMsg.NewEpochReady == nil:
			return errors.Errorf("message of type NewEpochReady, but new_epoch_ready field is nil")
		case innerMsg.NewEpochReady.Config == nil:
			return errors.Errorf("NewEpochReady has nil Config")
		}
	default:
		return errors.Errorf("unknown type '%T' for message", outerMsg.Type)
	}

	return nil
}
