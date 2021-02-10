package reqstore_test

import (
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/reqstore"
)

var _ = Describe("Reqstore", func() {
	var (
		tmpDir   string
		reqStore *reqstore.Store

		ack1dot1 = &pb.RequestAck{
			ClientId: 1,
			ReqNo:    1,
			Digest:   []byte("digest1"),
		}

		ack1dot2 = &pb.RequestAck{
			ClientId: 1,
			ReqNo:    2,
			Digest:   []byte("digest1"),
		}

		ack1dot3 = &pb.RequestAck{
			ClientId: 1,
			ReqNo:    3,
			Digest:   []byte("digest1"),
		}

		ack2dot1 = &pb.RequestAck{
			ClientId: 2,
			ReqNo:    1,
			Digest:   []byte("digest1"),
		}

		ack2dot2 = &pb.RequestAck{
			ClientId: 2,
			ReqNo:    2,
			Digest:   []byte("digest1"),
		}
	)

	BeforeEach(func() {
		var err error

		tmpDir, err = ioutil.TempDir("", "reqstore-test-*")
		Expect(err).NotTo(HaveOccurred())

		reqStore, err = reqstore.Open(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		err = reqStore.PutRequest(ack1dot1, []byte("data1dot1"))
		Expect(err).NotTo(HaveOccurred())

		err = reqStore.PutRequest(ack1dot2, []byte("data1dot2"))
		Expect(err).NotTo(HaveOccurred())

		err = reqStore.PutRequest(ack1dot3, []byte("data1dot3"))
		Expect(err).NotTo(HaveOccurred())

		err = reqStore.PutRequest(ack2dot1, []byte("data2dot1"))
		Expect(err).NotTo(HaveOccurred())

		err = reqStore.PutRequest(ack2dot2, []byte("data2dot2"))
		Expect(err).NotTo(HaveOccurred())

		err = reqStore.Commit(ack1dot1)
		Expect(err).NotTo(HaveOccurred())

		err = reqStore.Commit(ack1dot2)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		reqStore.Close()
		os.RemoveAll(tmpDir)
	})

	It("returns all uncommitted txes", func() {
		_ = reqStore
		// XXX need to actually test this
	})
})
