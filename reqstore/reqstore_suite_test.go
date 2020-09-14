package reqstore_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestReqstore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Reqstore Suite")
}
