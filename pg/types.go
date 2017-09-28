// Copyright © 2017 Joyent, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pg

import "github.com/alecthomas/units"

type (
	OID uint64

	// HeapBlockNumber represents a given HeapBlockNumber inside of a segment
	HeapBlockNumber   uint64
	HeapPageNumber    uint64
	HeapSegmentNumber uint32

	TimelineID uint32
)

const (
	InvalidTimelineID TimelineID = 0

	HeapPageSize = WALPageSize

	// HeapMaxSegmentSize is the max size of a single file in a relation.
	//
	// TODO(seanc@): pull this value from: `SELECT blocks_per_segment FROM
	// pg_control_init()` and multiply it by HeapPageSize.
	HeapMaxSegmentSize = 1 * units.GiB
)

// HeapSegmentPageNum returns the page number of a given page inside of a heap
// segment.
func HeapSegmentPageNum(block HeapBlockNumber) HeapPageNumber {
	return HeapPageNumber(uint64(block) % uint64(HeapMaxSegmentSize/HeapPageSize))
}

// SegmentNumber returns a HeapSegmentNumber corresponding to the SegmentNumber
// for a given relation.
func (heapBlockNo HeapBlockNumber) SegmentNumber() HeapSegmentNumber {
	return HeapSegmentNumber(uint64(heapBlockNo) / uint64(HeapMaxSegmentSize))
}
