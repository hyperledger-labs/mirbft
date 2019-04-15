package mirbft_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMirbft(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Mirbft Suite")
}
