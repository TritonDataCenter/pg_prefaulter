package lsn

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/alecthomas/units"
	"github.com/pkg/errors"
)

type LSN uint64

const (
	// Value representing an invalid LSN (used in error conditions)
	InvalidLSN = LSN(math.MaxUint64)

	// WALPageSize defaults to 8KB
	WALPageSize = 8 * units.KiB

	// WALSegmentSize defaults to 16MB
	WALSegmentSize              = 16 * units.MiB
	WALSegmentsPerXLogId uint64 = 0x100000000 / uint64(WALSegmentSize)
)

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

	return LSN(uint64(id)<<32 | offset), nil
}

// ID returns the numeric ID of the WAL.
func (lsn LSN) ID() uint32 {
	return uint32(lsn >> 32)
}

// Offset returns the byte offset inside of a WAL segment.
func (lsn LSN) Offset() uint32 {
	return uint32(lsn)
}

// Segment returns the Segment number of the LSN.
func (lsn LSN) Segment() uint64 {
	return uint64(lsn) / uint64(WALSegmentSize)
}

// String returns the string representation of an LSN.
func (lsn LSN) String() string {
	var id, offset uint32
	id = uint32(lsn >> 32)
	offset = uint32(lsn)
	return fmt.Sprintf("%X/%X", id, offset)
}

// WALFileName returns the name of a WAL's filename.  The timeline number is
// optional.  If the timeline is not specified, default to a timelineID of 1.
func (lsn LSN) WALFileName(timelineID ...uint32) string {
	var tid uint32
	switch len(timelineID) {
	case 0:
		tid = 1
	case 1:
		tid = timelineID[0]
	default:
		panic("only one timelineID supported")
	}

	return fmt.Sprintf("%08X%08X%08X", tid,
		lsn.Segment()/WALSegmentsPerXLogId,
		lsn.Segment()%WALSegmentsPerXLogId)
}
