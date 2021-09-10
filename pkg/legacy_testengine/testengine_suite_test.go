/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// TODO: This is LEGACY CODE It is not intended to be used as-is.
//       Use this code as an inspiration for implementing similar functionnality

package legacy_testengine_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTestengine(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Testengine Suite")
}
