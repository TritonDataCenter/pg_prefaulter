package lsn

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/alecthomas/units"
	"github.com/pkg/errors"
)

type (
	LSN     uint64
	Segment uint32
	Offset  uint32

	TimelineID uint32
)

const (
	// Value representing an invalid LSN (used in error conditions)
	InvalidLSN = LSN(math.MaxUint64)

	// WALPageSize defaults to 8KB
	WALPageSize = 8 * units.KiB

	// WALSegmentSize defaults to 16MB
	WALSegmentSize              = 16 * units.MiB
	WALSegmentsPerXLogId uint64 = 0x100000000 / uint64(WALSegmentSize)
)

// New creates a new LSN from a segment ID and offset
func New(segNo Segment, off Offset) LSN {
	return LSN(uint64(segNo)<<32 | uint64(off))
}

// Parse returns a parsed LSN
func Parse(in string) (LSN, error) {
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

	return New(Segment(id), Offset(offset)), nil
}

// ID returns the numeric ID of the WAL.
func (lsn LSN) ID() Segment {
	return Segment(uint32(lsn >> 32))
}

// Offset returns the byte offset inside of a WAL segment.
func (lsn LSN) ByteOffset() Offset {
	return Offset(lsn)
}

// Segment returns the Segment number of the LSN.
func (lsn LSN) SegmentNumber() Segment {
	return Segment(uint64(lsn) / uint64(WALSegmentSize))
}

// String returns the string representation of an LSN.
func (lsn LSN) String() string {
	var segNo Segment
	var off Offset
	segNo = Segment(lsn >> 32)
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
		uint64(lsn.SegmentNumber())/WALSegmentsPerXLogId,
		uint64(lsn.SegmentNumber())%WALSegmentsPerXLogId)
}
