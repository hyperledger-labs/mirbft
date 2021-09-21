/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package modules

import "hash"

// TODO: Write comments.
type Hasher interface {
	New() hash.Hash
}
