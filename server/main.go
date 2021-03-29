/*
Copyright IBM Corp. 2021 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"os"
	"runtime"
	"strconv"

	"github.com/IBM/mirbft/backend"
	"github.com/IBM/mirbft/config"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("server-main")
var format = logging.MustStringFormatter(
	`%{time:15:04:05.000000} %{shortfunc} %{message}`,
)

func fatal(err error) {
	if err != nil {
		log.Error(err)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	configFile := os.Args[1]
	// Load config file
	config.LoadFile(configFile)

	// Get output file prefix from command line.
	// Log and trace files will all begin with this
	outFilePrefix := os.Args[2]

	if len(os.Args) > 3 {
		txNo, err := strconv.Atoi(os.Args[3])
		fatal(err)
		backend.StartServer(outFilePrefix, txNo)
	} else {
		backend.StartServer(outFilePrefix, 0)
	}

}
