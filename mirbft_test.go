/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft"
	"github.com/IBM/mirbft/consumer"

	"go.uber.org/zap"
)

var _ = Describe("MirBFT", func() {
	var (
		config *consumer.Config
		doneC  chan struct{}
	)

	BeforeEach(func() {
		logger, err := zap.NewDevelopment()
		Expect(err).NotTo(HaveOccurred())

		config = &consumer.Config{
			ID:     0,
			Logger: logger,
		}

		doneC = make(chan struct{})
	})

	AfterEach(func() {
		close(doneC)
	})

	Describe("StartNewNode", func() {
		It("returns an instance of mirbft", func() {
			node, err := mirbft.StartNewNode(config, doneC, []mirbft.Replica{{ID: 0}})
			Expect(err).NotTo(HaveOccurred())
			Expect(node).NotTo(BeNil())
		})
	})
})
