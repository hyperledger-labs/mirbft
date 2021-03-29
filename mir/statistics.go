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

package mir

import (
	"math"
	"sort"
	"time"
)

func mean(vector []time.Duration) int64 {
	if len(vector) > 0 {
		sum := int64(0)
		for _, value := range vector {
			sum = sum + value.Nanoseconds()
		}
		return sum / int64(len(vector))
	}
	return 0
}

func median(vector []time.Duration) int64 {
	if len(vector) > 0 {
		sort.Slice(
			vector, func(i, j int) bool {
				return vector[i].Nanoseconds() > vector[j].Nanoseconds()
			})
		mid := max(uint64(len(vector)/2), 1)
		return vector[mid].Nanoseconds()
	}
	return 0
}

func std(vector []time.Duration) int64 {
	var sd float64

	L := len(vector)
	mean := float64(mean(vector))
	for j := 0; j < L; j++ {
		val := float64(vector[j].Nanoseconds())
		// The use of Pow math function func Pow(x, y float64) float64
		sd += math.Pow(val-mean, 2)
	}
	// The use of Sqrt math function func Sqrt(x float64) float64
	sd = math.Sqrt(sd / 10)
	return int64(math.Ceil(sd))
}
