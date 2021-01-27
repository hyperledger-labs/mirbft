package testengine_test

import (
	"bytes"
	"compress/gzip"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft/testengine"
)

var _ = Describe("Player", func() {
	var (
		recorder   *testengine.Recorder
		recording  *testengine.Recording
		serialized *bytes.Buffer
	)

	BeforeEach(func() {
		serialized = &bytes.Buffer{}
		gzw := gzip.NewWriter(serialized)
		defer gzw.Close()

		recorder = testengine.BasicRecorder(4, 4, 20)
		recorder.NetworkState.Config.MaxEpochLength = 200000 // XXX this works around a bug in the library for now

		var err error
		recording, err = recorder.Recording(gzw)
		Expect(err).NotTo(HaveOccurred())

		_, err = recording.DrainClients(50000)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Executes and produces a log", func() {
		el, err := testengine.ReadEventLog(serialized)
		Expect(err).NotTo(HaveOccurred())

		player, err := testengine.NewPlayer(el, os.Stdout)
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
