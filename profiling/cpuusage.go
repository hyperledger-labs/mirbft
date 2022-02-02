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
	"math"
	"time"

	linuxproc "github.com/c9s/goprocinfo/linux"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/tracing"
)

func StartCPUTracing(trace tracing.Trace, interval time.Duration) {
	// TODO: Implement a graceful shutdown
	go func() {
		for {
			usage := GetCPUUsage([]string{"Load", "System"}, interval)
			trace.Event(tracing.CPU_USAGE, int64(math.Round(float64(usage[0]*100))), int64(math.Round(float64(usage[1])*100)))
		}
	}()
}

// Measures average CPU usage in the given time window and returns it as a floating point number between 0 and 1.
// ATTENTION: Blocks for the duration of the time window!
func GetCPUUsage(fields []string, window time.Duration) []float32 {

	// Get initial CPU stats
	stat, err := linuxproc.ReadStat("/proc/stat")
	if err != nil {
		logger.Fatal().Err(err).Msg("Could not read statistics.")
	}
	oldCPUStat := stat.CPUStatAll

	// Wait for given time window
	time.Sleep(window)

	// Get new CPU stats
	stat, err = linuxproc.ReadStat("/proc/stat")
	if err != nil {
		logger.Fatal().Err(err).Msg("Could not read statistics.")
	}
	newCPUStat := stat.CPUStatAll

	// Compute changes that occurred in the meantime
	diff := diffCPUStat(newCPUStat, oldCPUStat)

	// Get relevant fields from the diff
	result := make([]float32, len(fields), len(fields))
	for i, field := range fields {

		var cpuVal uint64
		switch field {
		case "User":
			cpuVal = diff.User
		case "Nice":
			cpuVal = diff.Nice
		case "System":
			cpuVal = diff.System
		case "Idle":
			cpuVal = diff.Idle
		case "IOWait":
			cpuVal = diff.IOWait
		case "IRQ":
			cpuVal = diff.IRQ
		case "SoftIRQ":
			cpuVal = diff.SoftIRQ
		case "Steal":
			cpuVal = diff.Steal
		case "Guest":
			cpuVal = diff.Guest
		case "GuestNice":
			cpuVal = diff.GuestNice
		case "Load":
			cpuVal = diff.User + // Sum of all except Idle
				diff.Nice +
				diff.System +
				diff.IOWait +
				diff.IRQ +
				diff.SoftIRQ +
				diff.Steal +
				diff.Guest +
				diff.GuestNice
		}

		if sumCPUStat(diff) == 0 {
			result[i] = 0
		} else {
			result[i] = (float32(cpuVal) / float32(sumCPUStat(diff)))
		}
	}

	return result
}

func sumCPUStat(stat linuxproc.CPUStat) uint64 {
	return stat.User +
		stat.Nice +
		stat.System +
		stat.Idle +
		stat.IOWait +
		stat.IRQ +
		stat.SoftIRQ +
		stat.Steal +
		stat.Guest +
		stat.GuestNice
}

func diffCPUStat(first linuxproc.CPUStat, second linuxproc.CPUStat) linuxproc.CPUStat {
	return linuxproc.CPUStat{
		Id:        first.Id,
		User:      first.User - second.User,
		Nice:      first.Nice - second.Nice,
		System:    first.System - second.System,
		Idle:      first.Idle - second.Idle,
		IOWait:    first.IOWait - second.IOWait,
		IRQ:       first.IRQ - second.IRQ,
		SoftIRQ:   first.SoftIRQ - second.SoftIRQ,
		Steal:     first.Steal - second.Steal,
		Guest:     first.Guest - second.Guest,
		GuestNice: first.GuestNice - second.GuestNice,
	}
}
