package main_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

//test suit

func TestMircat(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Mircat Suite")
}
