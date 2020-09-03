/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ContextTimeout is an unfortunate parameter included for executing
// the stress related tests.  Most of the testing we try to make deterministic
// and independent of time (for instance, by specifying step counts), but for
// the more 'real' integration stress tests, this is not possible.  Since
// the CI hardware is weak, and, the race detector slows testing considerably,
// this value is overridded via MIRBFT_TEST_CONTEXT_TIMEOUT in CI.
var ContextTimeout = 30 * time.Second

func TestMirbft(t *testing.T) {
	RegisterFailHandler(Fail)

	val := os.Getenv("MIRBFT_TEST_CONTEXT_TIMEOUT")
	if val != "" {
		dur, err := time.ParseDuration(val)
		Expect(err).NotTo(HaveOccurred())
		ContextTimeout = dur
	}

	RunSpecs(t, "Mirbft Suite")
}
