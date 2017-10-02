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
	"github.com/alecthomas/units"
)

type (
	WAL struct {
		TimelineID
		WALSegmentNumber
	}

	// WALFilename is a hex-encoded tripple with the following encoding:
	//
	//   0000ABCD0012345600000078
	//   ^^^^^^^^
	//      |    ^^^^^^^^^^^^^^^^
	//      |        |
	//   TimelineID  |
	//      SegmentNumber()
	//
	//
	// Where the TimelineID is 0x0000ABCD and the SegmentNumber is 0x12345678
	//
	// The SegmentNumber() is split across two 8 byte nibbles.  The high 32 bits
	// are stored in the second nibble.  The third tripple contains the upper byte
	// of the lower 32 bits.  This is a different way of saying the SegmentNumber
	// is a 40 bit number encoded across two 32 bit unsigned integers, however in
	// practice the SegmentNumber can't exceed 32 bits so in reality it's actually
	// just a 32 bit unsigned integer.  An LSN can encode the full 40 bits (an LSN
	// is a 64 bit uint) , but references to segment numbers are only 32 bit
	// unsigned ints.  Why is it this way?  Don't ask me, go ask your mother.
	WALFilename string

	// WALByteOffset is the byte offset portion of an LSN.  The WALByteOffset is
	// by a 24 bit number by default (16MiB).  Values exceeding the
	// WALMaxByteOffset will be implicitly truncated using the LSNByteMask.
	// Changing this value is highly discouraged.
	WALByteOffset uint32

	// WALSegmentNumber is the segment number portion of an LSN.  The
	// WALSegmentNumber is a 40 bit number.  Values exceeding WALMaxSegmentNumber
	// will be implicit truncated using the LSNSegmentMask.
	WALSegmentNumber uint64

	WALFiles []WALFilename
)

const (
	// WALPageSize == PostgreSQL's Page Size.  Page Size == BLKSZ WALPageSize
	// defaults to 8KB.  The same value used in the WAL is used in the PostgreSQL
	// Heap.
	//
	// TODO(seanc@): pull this data from `SELECT database_block_size FROM pg_control_init()`
	WALPageSize = 8 * units.KiB

	// WALSegmentSize == PostgreSQL WAL File Size.
	// WALSegmentSize defaults to 16MB
	//
	// TODO(seanc@): pull this value from `SELECT bytes_per_wal_segment FROM pg_control_init()`
	WALSegmentSize = 16 * units.MiB

	// WALMaxByteOffset == WALSegmentSize-1 (2^24 - 1)
	WALMaxByteOffset WALByteOffset = 1<<24 - 1

	// WALMaxSegmentNumber == WALMaxSegmentNumber-1 (2^40 - 1)
	WALMaxSegmentNumber WALSegmentNumber = 1<<40 - 1

	// #define XLogSegmentsPerXLogId   (UINT64CONST(0x100000000) / XLOG_SEG_SIZE)
	WALSegmentsPerWALID uint64 = (1 << 32) / uint64(WALSegmentSize)
)

func NewWAL() WAL {
	return WAL{
		// The timeline is always initialized to 1
		TimelineID:       1,
		WALSegmentNumber: 0,
	}
}

// Unique returns a set of unique WAL files, deduplicating the inputs.
// The resulting set is in a random order.
func (walFiles WALFiles) Unique() WALFiles {
	m := make(map[WALFilename]struct{}, len(walFiles))
	for n := range walFiles {
		m[walFiles[n]] = struct{}{}
	}

	uniq := make([]WALFilename, 0, len(m))
	for k := range m {
		uniq = append(uniq, k)
	}

	return uniq
}
