/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package testengine provides a way to write deterministic, repeatable,
// serializable, and analyzable tests.  Tests are deterministic because
// all time is fake, there is only ever a single go routine executing,
// and all randomness is derived from a seed.  Tests are repeatable because
// given the same configuration and random seed, the test actions are
// deterministic.  Tests are serializable because the actions performed
// are all represented as protobuf messages.  And finally, tests are analyzable
// because the serialized representation of the test actions may be analyzed
// in tooling after the test has executed.
// TODO, the above is the plan, but, nothing is implemented at this point.
package testengine
