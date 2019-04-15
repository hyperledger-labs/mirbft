/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ibm/mirbft"
)

var _ = Describe("MirBFT", func() {
	var (
		ab *mirbft.AtomicBroadcast
	)

	BeforeEach(func() {
		ab = &mirbft.AtomicBroadcast{}
	})

	Describe("Propose", func() {
		It("returns a placeholder error", func() {
			err := ab.Propose()
			Expect(err).To(MatchError("unimplemented"))
		})
	})
})
