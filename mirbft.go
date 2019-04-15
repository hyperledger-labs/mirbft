/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"github.com/pkg/errors"
)

type AtomicBroadcast struct{}

func (ab *AtomicBroadcast) Propose() error {
	return errors.Errorf("unimplemented")
}
