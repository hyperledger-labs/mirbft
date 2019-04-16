/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ibm/mirbft"

	"go.uber.org/zap"
)

var _ = Describe("MirBFT", func() {
	var (
		ab     *mirbft.AtomicBroadcast
		config *mirbft.Config
	)

	BeforeEach(func() {
		logger, err := zap.NewDevelopment()
		Expect(err).NotTo(HaveOccurred())

		config = &mirbft.Config{
			ID:     1,
			Logger: logger,
		}

		ab = &mirbft.AtomicBroadcast{
			Config: config,
		}
	})

	Describe("Propose", func() {
		It("returns a placeholder error", func() {
			err := ab.Propose()
			Expect(err).To(MatchError("unimplemented"))
		})
	})
})
