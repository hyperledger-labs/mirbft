package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

//tests for main.go

var _ = Describe("pargsArgs", func() {
	It("parses a populated command line", func() {
		args, err := parseArgs([]string{
			"--src", "main.go",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(args.srcFile).NotTo(BeNil())

	})
	When("src input is not provided", func() {
		It("returns an error", func() {
			_, err := parseArgs([]string{})
			Expect(err).To(MatchError("required input \" --src <Src_File> \" not found !"))
		})
	})
})
