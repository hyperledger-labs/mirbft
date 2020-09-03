package testengine_test

import (
	"bytes"
	"compress/gzip"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft/testengine"
	"go.uber.org/zap"
)

var _ = Describe("Player", func() {
	var (
		logger     *zap.Logger
		recorder   *testengine.Recorder
		recording  *testengine.Recording
		serialized *bytes.Buffer
	)

	BeforeEach(func() {
		serialized = &bytes.Buffer{}
		gzw := gzip.NewWriter(serialized)
		defer gzw.Close()

		var err error
		logger, err = zap.NewProduction()
		Expect(err).NotTo(HaveOccurred())

		recorder = testengine.BasicRecorder(4, 4, 20)
		recorder.NetworkState.Config.MaxEpochLength = 200000 // XXX this works around a bug in the library for now

		recording, err = recorder.Recording(gzw)
		Expect(err).NotTo(HaveOccurred())

		_, err = recording.DrainClients(50000)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		logger.Sync()

	})

	It("Executes and produces a log", func() {
		el, err := testengine.ReadEventLog(serialized)
		Expect(err).NotTo(HaveOccurred())

		player, err := testengine.NewPlayer(el, logger)
		Expect(err).NotTo(HaveOccurred())

		for el.List.Len() > 0 {
			err = player.Step()
			Expect(err).NotTo(HaveOccurred())
		}

		for i := uint64(0); i < 4; i++ {
			Expect(player.Nodes[i].Status).To(Equal(recording.Nodes[i].PlaybackNode.Status))
		}
	})
})
