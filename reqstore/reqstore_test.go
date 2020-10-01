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

		err = reqStore.Store(ack1dot1, []byte("data1dot1"))
		Expect(err).NotTo(HaveOccurred())

		err = reqStore.Store(ack1dot2, []byte("data1dot2"))
		Expect(err).NotTo(HaveOccurred())

		err = reqStore.Store(ack1dot3, []byte("data1dot3"))
		Expect(err).NotTo(HaveOccurred())

		err = reqStore.Store(ack2dot1, []byte("data2dot1"))
		Expect(err).NotTo(HaveOccurred())

		err = reqStore.Store(ack2dot2, []byte("data2dot2"))
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
		count := 0
		reqStore.Uncommitted(func(ack *pb.RequestAck) {
			switch ack.ReqNo {
			case 1:
				Expect(ack).To(Equal(ack2dot1))
			case 2:
				Expect(ack).To(Equal(ack2dot2))
			case 3:
				Expect(ack).To(Equal(ack1dot3))
			default:
				Fail("unexpected ack reqno")
			}
			count++
		})
		Expect(count).To(Equal(3))
	})
})
