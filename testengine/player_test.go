package testengine_test

import (
	"bytes"
	"time"

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
		var err error
		logger, err = zap.NewProduction()
		Expect(err).NotTo(HaveOccurred())

		recorder = testengine.BasicRecorder(4, 4, 20)
		recorder.NetworkConfig.MaxEpochLength = 100000 // XXX this works around a bug in the library for now

		recording, err = recorder.Recording()
		Expect(err).NotTo(HaveOccurred())

		_, err = recording.DrainClients(10 * time.Second)
		Expect(err).NotTo(HaveOccurred())

		serialized = &bytes.Buffer{}
		err = recording.EventLog.Write(serialized)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if recording != nil && recording.Player != nil && recording.Player.DoneC != nil {
			close(recording.Player.DoneC)
		}

		logger.Sync()

	})

	It("Executes and produces a log", func() {
		el, err := testengine.ReadEventLog(serialized)
		Expect(err).NotTo(HaveOccurred())

		player, err := testengine.NewPlayer(el, logger)
		Expect(err).NotTo(HaveOccurred())

		for el.NextEventLogEntry != nil {
			err = player.Step()
			Expect(err).NotTo(HaveOccurred())
		}

		for i := 0; i < 4; i++ {
			Expect(player.Nodes[i].Status).To(Equal(recording.Nodes[i].PlaybackNode.Status))
		}
	})
})
