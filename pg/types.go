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

import (
	"math"

	"github.com/alecthomas/units"
)

type (
	OID uint64

	// HeapBlockNumber represents a given HeapBlockNumber inside of a segment
	HeapBlockNumber uint64
	HeapPageNumber  uint64
)

type (
	HeapSegment uint32
	Offset      uint32

	TimelineID uint32

	WALFilename string
)

const (
	// Value representing an invalid LSN (used in error conditions)
	InvalidLSN = LSN(math.MaxUint64)

	// WALPageSize == PostgreSQL's Page Size.  Page Size == BLKSZ WALPageSize
	// defaults to 8KB.  The same value used in the WAL is used in the PostgreSQL
	// Heap.
	//
	// TODO(seanc@): pull this data from `SHOW wal_block_size`.
	WALPageSize = 8 * units.KiB

	HeapPageSize = WALPageSize

	// WALFileSize == PostgreSQL WAL File Size.
	// WALFileSize defaults to 16MB
	//
	// TODO(seanc@): pull this value from `SHOW wal_segment_size`
	WALFileSize = 16 * units.MiB

	// HeapMaxSegmentSize is the max size of a single file in a relation.
	//
	// TODO(seanc@): pull this value from pg_controldata(1)'s "Blocks per segment
	// of large relation" and multiply it by WALBlockSize
	HeapMaxSegmentSize = 1 * units.GiB

	WALFilesPerSegment uint64 = 0x100000000 / uint64(WALFileSize)
)

// HeapSegmentPageNum returns the page number of a given page inside of a heap
// segment.
func HeapSegmentPageNum(block HeapBlockNumber) HeapPageNumber {
	return HeapPageNumber(uint64(block) % uint64(HeapMaxSegmentSize/HeapPageSize))
}

// SegmentNumber returns the segment number from a given block number.
func SegmentNumber(block uint64) HeapSegment {
	return HeapSegment(int64(block) / int64(HeapMaxSegmentSize/WALPageSize))
}
