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
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/alecthomas/units"
	"github.com/pkg/errors"
)

// LSN is a Go implementation of PostgreSQL's Log Sequence Number (LSN):
// https://www.postgresql.org/docs/current/static/datatype-pg-lsn.html
//
// An LSN encodes two pieces of information:
//
// 1. the WAL Segment
// 2. the byte offset within an individual WAL segment
//
// The byte offset is the lower 24 bits of the LSN.  The WAL Segment number is
// the uppwer 40 bits of the LSN.
type LSN uint64

const (
	LSNByteMask    WALByteOffset    = 0x00FFFFFF
	LSNSegmentMask WALSegmentNumber = 0xFFFFFFFFFF
)

// NewLSN creates a new LSN from a segment ID and offset
func NewLSN(segNo WALSegmentNumber, off WALByteOffset) LSN {
	return LSN(uint64(LSNSegmentMask&segNo)<<24 | uint64(LSNByteMask&off))
}

// LSNCmp compares x and y and returns:
//
//
//   -1 if x <  y
//    0 if x == y
//   +1 if x >  y
func LSNCmp(x, y LSN) int {
	xInt := uint64(x)
	yInt := uint64(y)

	switch {
	case xInt < yInt:
		return -1
	case xInt == yInt:
		return 0
	case xInt > yInt:
		return 1
	default:
		panic(fmt.Sprintf("cmp fail: %+q/%q", x, y))
	}
}

// MustParseLSN returns a parsed LSN.  If the LSN input fails to parse,
// MustParseLSN will panic.
func MustParseLSN(inLSN string) LSN {
	lsn, err := ParseLSN(inLSN)
	if err != nil {
		panic(fmt.Sprintf("bad LSN input: %+q", inLSN))
	}

	return lsn
}

// ParseLSN returns a parsed LSN
func ParseLSN(in string) (LSN, error) {
	// Search for at most 3 parts so we can detect if the input was malformed.
	parts := strings.SplitN(in, "/", 3)
	if len(parts) != 2 {
		return InvalidLSN, fmt.Errorf("invalid LSN: %q", in)
	}

	segNo, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return InvalidLSN, errors.Wrap(err, "unable to decode the WAL segment number")
	}

	offset, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return InvalidLSN, errors.Wrap(err, "unable to decode the WAL offset")
	}

	// NOTE(seanc@): the order of the AND NOT operator is significant.  See the
	// type LSN comment for details on the extraction.
	segNo = uint64(segNo<<8) | uint64((WALByteOffset(offset)&^LSNByteMask)>>24)

	return NewLSN(WALSegmentNumber(segNo), LSNByteMask&WALByteOffset(offset)), nil
}

// ParseWalfile returns a parsed LSN from a given WALFilename
func ParseWalfile(in WALFilename) (TimelineID, LSN, error) {
	if len(in) != 24 {
		return InvalidTimelineID, InvalidLSN, fmt.Errorf("WAL Filename incorrect: %+q", in)
	}

	timelineID, err := strconv.ParseUint(string(in)[:8], 16, 64)
	if err != nil {
		return InvalidTimelineID, InvalidLSN, errors.Wrap(err, "unable to decode the timeline ID")
	}

	segmentHigh, err := strconv.ParseUint(string(in)[8:16], 16, 64)
	if err != nil {
		return InvalidTimelineID, InvalidLSN, errors.Wrap(err, "unable to decode the WAL segment high bits")
	}

	segmentLow, err := strconv.ParseUint(string(in)[16:24], 16, 64)
	if err != nil {
		return InvalidTimelineID, InvalidLSN, errors.Wrap(err, "unable to decode the WAL segment low bits")
	}

	// Cast to 32 bit in order to explicitly receive the side effects of integer
	// overflow/wrap around.
	segmentHigh32 := uint32(segmentHigh)
	segmentLow32 := uint32(segmentLow)
	lsn := LSN(
		((uint64(segmentHigh32) << 32) |
			(uint64(segmentLow32) * uint64(WALSegmentSize))) + 1)

	return TimelineID(timelineID), lsn, nil
}

// AddBytes adds bytes to a given LSN
func (lsn LSN) AddBytes(n units.Base2Bytes) LSN {
	return LSN(uint64(lsn) + uint64(n))
}

// Offset returns the byte offset inside of a WAL segment.
func (lsn LSN) ByteOffset() WALByteOffset {
	return WALByteOffset(uint64(lsn) % uint64(WALSegmentSize))
}

// Readahead returns all of the anticipated WAL filenames that will be present
// in the future based on the lsn and the readahead.
func (lsn LSN) Readahead(timelineID TimelineID, maxBytes units.Base2Bytes) WALFiles {
	// Always read in at least the current WAL file
	walFiles := make(WALFiles, 0, int(math.Ceil(float64(maxBytes)/float64(WALSegmentSize))))

	cur := lsn
	for remainingBytes := maxBytes; remainingBytes > 0; remainingBytes -= WALSegmentSize {
		walFiles = append(walFiles, cur.WALFilename(timelineID))
		cur = cur.AddBytes(WALSegmentSize)
	}

	return walFiles
}

// String returns the string representation of an LSN.
func (lsn LSN) String() string {
	segNo := lsn.SegmentNumber()
	return fmt.Sprintf("%X/%X%X", segNo.High(), segNo.Low(), lsn.ByteOffset())
}

// SegmentNumber returns the Segment number of the LSN.
func (lsn LSN) SegmentNumber() WALSegmentNumber {
	return WALSegmentNumber(uint64(lsn) / uint64(WALSegmentSize))
}

// WALFilename returns the name of a WAL's filename.  The timeline number is
// optional.  If the timeline is not specified, default to a timelineID of 1.
func (lsn LSN) WALFilename(timelineID TimelineID) WALFilename {
	// NOTE(seanc@): The -1 comes from PostgreSQL's XLByteToPrevSeg(), which is
	// used by XLogFileName().  See postgresql/src/include/access/xlog_internal.h
	// for additional details.
	prevLSN := lsn - 1
	prevSeg := prevLSN.SegmentNumber()
	walFilename := fmt.Sprintf("%08X%08X%08X",
		timelineID,
		prevSeg.High(),
		prevSeg.Low())
	return WALFilename(walFilename)
}

// High returns the high 32 bits of the SegmentNumber.
func (segNo WALSegmentNumber) High() uint64 {
	return uint64(uint32(uint64(segNo) / uint64(WALSegmentsPerWALID)))
}

// LowSegmentNumberLow returns the lower 8 bits of the SegmentNumber.
func (segNo WALSegmentNumber) Low() uint64 {
	return uint64(uint32(uint64(segNo) % uint64(WALSegmentsPerWALID)))
}
