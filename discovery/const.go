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

package discovery

const (
	PeerBasePort    = 10000
	WildcardAllTags = "__all__" // Used by the master only, and thus not part of the slave wildcard replacement.

	// These wildcards will be replaced in command arguments
	// and output file name by the corresponding values
	// when the slave executes the exec-start master command
	// (not to be confused by the command to execute).
	WildcardSlaveID   = "__id__"
	WildcardPublicIP  = "__public_ip__"
	WildcardPrivateIP = "__private_ip__"
)
