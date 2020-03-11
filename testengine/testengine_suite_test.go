package testengine_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTestengine(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Testengine Suite")
}
