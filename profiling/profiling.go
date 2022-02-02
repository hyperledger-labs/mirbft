// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package profiling

import (
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"

	logger "github.com/rs/zerolog/log"
)

var (
	// Stores which profiler (key) should be dumped in which file (value)
	// For valid profiler names see the "runtime" package documentation.
	// This implementation supports "cpu" as a special name for CPU profiling
	profileOutputs = make(map[string]string)

	// Stores the open file to which pprof continuously writes data.
	cpuProfileFile *os.File = nil

	// Channel used by StopOnSignal() for OS signal redirection.
	sigChan = make(chan os.Signal)
)

// Starts the profiler. StopProfiler needs to be called to finish profiling.
// For valid profiler names see the "runtime" package documentation.
// This implementation supports "cpu" as a special name for CPU profiling
// The outFileName argument designates the name of the output file to which profile data will be written after profiler
// is stopped.
// The rate is used for setting the "block" and "mutex" profile rate / fraction. Ignored for other names.
func StartProfiler(name string, outFileName string, rate int) {

	// Save output file name.
	profileOutputs[name] = outFileName

	// Perform additional actions for "block", "mutex", and "cpu" profilers.
	// "block" and "mutex" need to be explicitly enabled and "cpu" needs to be explicitly started with an open file.
	switch name {
	case "block":
		// Enable block profiler
		runtime.SetBlockProfileRate(rate)
		logger.Info().Msg("Started Profiler: BLOCK")
	case "mutex":
		// Enable mutex profiler
		runtime.SetMutexProfileFraction(rate)
		logger.Info().Msg("Started Profiler: MUTEX")
	case "cpu":
		// Open CPU profiler output file
		var err error
		cpuProfileFile, err = os.Create(outFileName)
		if err != nil {
			logger.Fatal().Err(err).Str("fileName", outFileName).Msg("Could not create CPU profile.")
		}
		// Start CPU Profiler
		if err := pprof.StartCPUProfile(cpuProfileFile); err != nil {
			logger.Fatal().Err(err).Msg("Could not start CPU profile.")
		}
		logger.Info().Msg("Started Profiler: CPU")
	}
}

// Stops all profilers started by StartProfiler() and dumps their output to their corresponding files.
func StopProfiler() {

	logger.Info().Msg("Stopping profiler.")

	// Disable the "block" and "mutex" profilers
	runtime.SetBlockProfileRate(0)
	runtime.SetMutexProfileFraction(0)

	// Dump profiler data into files.
	for name, fileName := range profileOutputs {
		if name == "cpu" {
			pprof.StopCPUProfile()
			if err := cpuProfileFile.Close(); err != nil {
				logger.Error().Err(err).Str("fileName", fileName).Msg("Could not close CPU profile output.")
			}
			logger.Info().Str("name", name).Str("filename", fileName).Msg("Profile data written.")
		} else {
			dumpProfile(name, fileName)
		}
		delete(profileOutputs, name)
	}
}

// Sets up a signal handler that calls StopProfiler when the specified OS signal occurs.
// This is useful for profiling if the program does not terminate gracefully, but is stopped using an OS signal.
// StopOnSignal() can be used in this case to still obtain the profiler data.
// If the exit flag is set to true, the signal handler will exit the process after stopping the profiler.
func StopOnSignal(sig os.Signal, exit bool) {

	logger.Info().Bool("exit", exit).Msg("Profiler will stop on signal.")

	// Set up signal handler
	signal.Notify(sigChan, sig)

	// Start background goroutine waiting for the signal
	go func() {

		// Wait for signal
		<-sigChan

		// Stop profiler
		StopProfiler()

		// Exit if requested
		if exit {
			os.Exit(1)
		}
	}()

}

// Saves the data of profiler name to file named fileName.
func dumpProfile(name string, fileName string) {

	// Open profiler output file.
	f, err := os.Create(fileName)
	if err != nil {
		logger.Fatal().Err(err).Str("fileName", fileName).Msg("Could not open profile output file.")
	}

	// Flush profiler data.
	if err := pprof.Lookup(name).WriteTo(f, 1); err != nil {
		logger.Error().Str("fileName", fileName).Msg("Failed to write profile data to file.")
	}

	// Close profiler output file.
	if err := f.Close(); err != nil {
		logger.Error().Err(err).Str("fileName", fileName).Msg("Could not close profile output file.")
	}

	logger.Info().Str("name", name).Str("filename", fileName).Msg("Profile data written.")
}
