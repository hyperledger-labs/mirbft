package recorder_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRecorder(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Recorder Suite")
}
