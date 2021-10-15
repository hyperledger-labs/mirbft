/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package logging

import (
	"fmt"
)

// Logger is minimal logging interface designed to be easily adaptable to any
// logging library.
type Logger interface {
	// Log is invoked with the log level, the log message, and key/value pairs
	// of any relevant log details. The keys are always strings, while the
	// values are unspecified.
	Log(level LogLevel, text string, args ...interface{})
}

type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

// Simple console logger writing log messages directly to standard output.
type consoleLogger LogLevel

// Log is invoked with the log level, the log message, and key/value pairs
// of any relevant log details. The keys are always strings, while the
// values are unspecified. If the level is greater of equal than this consoleLogger,
// Log() writes the log message to standard output.
func (l consoleLogger) Log(level LogLevel, text string, args ...interface{}) {
	if level < LogLevel(l) {
		return
	}

	fmt.Print(text)
	for i := 0; i < len(args); i++ {
		if i+1 < len(args) {
			switch args[i+1].(type) {
			case []byte:
				// Print byte arrays in base 16 encoding.
				fmt.Printf(" %s=%x", args[i], args[i+1])
			default:
				// Print all other types using the Go default format.
				fmt.Printf(" %s=%v", args[i], args[i+1])
			}
			i++
		} else {
			fmt.Printf(" %s=%%MISSING%%", args[i])
		}
	}
	fmt.Printf("\n")
}

// The nil logger drops all messages.
type nilLogger struct{}

// The Log method of the nilLogger does nothing, effectively dropping every log message.
func (nl *nilLogger) Log(level LogLevel, text string, args ...interface{}) {
	// Do nothing.
}

var (
	// ConsoleDebugLogger implements Logger and writes all log messages to stdout.
	ConsoleDebugLogger Logger = consoleLogger(LevelDebug)

	// ConsoleInfoLogger implements Logger and writes all LevelInfo and above log messages to stdout.
	ConsoleInfoLogger Logger = consoleLogger(LevelInfo)

	// ConsoleWarnLogger implements Logger and writes all LevelWarn and above log messages to stdout.
	ConsoleWarnLogger Logger = consoleLogger(LevelWarn)

	// ConsoleErrorLogger implements Logger and writes all LevelError log messages to stdout.
	ConsoleErrorLogger Logger = consoleLogger(LevelError)

	// NilLogger drops all log messages.
	NilLogger Logger = &nilLogger{}
)
