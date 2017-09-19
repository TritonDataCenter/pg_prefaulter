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
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// LSN is a Go implementation of PostgreSQL's Log Sequence Number (LSN):
// https://www.postgresql.org/docs/current/static/datatype-pg-lsn.html
type LSN uint64

// NewLSN creates a new LSN from a segment ID and offset
func NewLSN(segNo HeapSegment, off Offset) LSN {
	return LSN(uint64(segNo)<<32 | uint64(off))
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

	return NewLSN(HeapSegment(id), Offset(offset)), nil
}

// ID returns the numeric ID of the WAL.
func (lsn LSN) ID() HeapSegment {
	return HeapSegment(uint32(lsn >> 32))
}

// Offset returns the byte offset inside of a WAL segment.
func (lsn LSN) ByteOffset() Offset {
	return Offset(lsn)
}

// String returns the string representation of an LSN.
func (lsn LSN) String() string {
	var segNo HeapSegment
	var off Offset
	segNo = HeapSegment(lsn >> 32)
	off = Offset(lsn)
	return fmt.Sprintf("%X/%X", segNo, off)
}

// Segment returns the Segment number of the LSN.
func (lsn LSN) SegmentNumber() HeapSegment {
	return HeapSegment(uint64(lsn) / uint64(WALFileSize))
}

// WALFilename returns the name of a WAL's filename.  The timeline number is
// optional.  If the timeline is not specified, default to a timelineID of 1.
func (lsn LSN) WALFilename(timelineID ...TimelineID) WALFilename {
	var tid TimelineID
	switch len(timelineID) {
	case 0:
		tid = 1
	case 1:
		tid = timelineID[0]
	default:
		panic("only one timelineID supported")
	}

	walFilename := fmt.Sprintf("%08X%08X%08X", tid,
		uint64(lsn.SegmentNumber())/WALFilesPerSegment,
		uint64(lsn.SegmentNumber())%WALFilesPerSegment)
	return WALFilename(walFilename)
}
