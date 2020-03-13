package mirbft_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	_ "github.com/IBM/mirbft"
	"github.com/IBM/mirbft/testengine"
)

var _ = Describe("Mirbft", func() {
	var (
		recorder *testengine.Recorder
	)

	BeforeEach(func() {
		recorder = testengine.BasicRecorder(4, 4, 100)
	})

	It("delivers all requests", func() {
		recording, err := recorder.Recording()
		Expect(err).NotTo(HaveOccurred())

		_, err = recording.DrainClients(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())
	})
})
