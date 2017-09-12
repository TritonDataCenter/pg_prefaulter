package pg

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/alecthomas/units"
	"github.com/pkg/errors"
)

type (
	OID uint64

	// HeapBlockNumber represents a given HeapBlockNumber inside of a segment
	HeapBlockNumber uint64
	HeapPageNumber  uint64
)

type (
	LSN         uint64
	HeapSegment uint32
	Offset      uint32

	TimelineID uint32
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

// New creates a new LSN from a segment ID and offset
func New(segNo HeapSegment, off Offset) LSN {
	return LSN(uint64(segNo)<<32 | uint64(off))
}

// HeapSegmentPageNum returns the page number of a given page inside of a heap
// segment.
func HeapSegmentPageNum(block HeapBlockNumber) HeapPageNumber {
	return HeapPageNumber(uint64(block) % uint64(HeapMaxSegmentSize/HeapPageSize))
}

// ParseLSN returns a parsed LSN
func ParseLSN(in string) (LSN, error) {
	parts := strings.Split(in, "/")
	if len(parts) != 2 {
		return InvalidLSN, fmt.Errorf("invalid LSN: %q", in)
	}

	id, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return InvalidLSN, errors.Wrap(err, "unable to decode the segment ID")
	}

	offset, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return InvalidLSN, errors.Wrap(err, "unable to decode the segment ID")
	}

	return New(HeapSegment(id), Offset(offset)), nil
}

// ID returns the numeric ID of the WAL.
func (lsn LSN) ID() HeapSegment {
	return HeapSegment(uint32(lsn >> 32))
}

// Offset returns the byte offset inside of a WAL segment.
func (lsn LSN) ByteOffset() Offset {
	return Offset(lsn)
}

// Segment returns the Segment number of the LSN.
func (lsn LSN) SegmentNumber() HeapSegment {
	return HeapSegment(uint64(lsn) / uint64(WALFileSize))
}

// SegmentNumber returns the segment number from a given block number.
func SegmentNumber(block uint64) HeapSegment {
	return HeapSegment(int64(block) / int64(HeapMaxSegmentSize/WALPageSize))
}

// String returns the string representation of an LSN.
func (lsn LSN) String() string {
	var segNo HeapSegment
	var off Offset
	segNo = HeapSegment(lsn >> 32)
	off = Offset(lsn)
	return fmt.Sprintf("%X/%X", segNo, off)
}

// WALFileName returns the name of a WAL's filename.  The timeline number is
// optional.  If the timeline is not specified, default to a timelineID of 1.
func (lsn LSN) WALFileName(timelineID ...TimelineID) string {
	var tid TimelineID
	switch len(timelineID) {
	case 0:
		tid = 1
	case 1:
		tid = timelineID[0]
	default:
		panic("only one timelineID supported")
	}

	return fmt.Sprintf("%08X%08X%08X", tid,
		uint64(lsn.SegmentNumber())/WALFilesPerSegment,
		uint64(lsn.SegmentNumber())%WALFilesPerSegment)
}
