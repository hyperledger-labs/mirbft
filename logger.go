/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"go.uber.org/zap"
)

// Logger is the subset of the *zap.Logger which mirbft utilizes.
// It has been abstracted as interface to allow easier mocking and to
// make it possible to write a shim to support  other loggers if necessary.
type Logger interface {
	Debug(msg string, fields ...zap.Field)
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Panic(msg string, fields ...zap.Field)
}
