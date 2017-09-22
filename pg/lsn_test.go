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

package pg_test

import (
	"testing"

	"github.com/alecthomas/units"
	"github.com/joyent/pg_prefaulter/pg"
	"github.com/kylelemons/godebug/pretty"
)

// Test adding bytes values to an LSN
func TestAddBytes(t *testing.T) {
	tests := []struct {
		inLSN       string
		addBytes    units.Base2Bytes
		equalLSN    string
		timeline    pg.TimelineID
		outFilename pg.WALFilename
	}{
		{
			inLSN:       "0/0",
			addBytes:    0,
			equalLSN:    "0/0",
			timeline:    100,
			outFilename: "000000640000000000000000",
		},
		{
			inLSN:       "0/0",
			addBytes:    pg.WALFileSize,
			equalLSN:    "0/1",
			timeline:    101,
			outFilename: "000000650000000000000001",
		},
		{
			inLSN:       "FF/FF",
			addBytes:    2*pg.WALFileSize + 1,
			equalLSN:    "0/1",
			timeline:    102,
			outFilename: "00000066000000FF00000002",
		},
	}

	if diff := pretty.Compare(pg.WALPageSize, 8192); diff != "" {
		t.Fatalf("WALPageSize diff: (-got +want)\n%s", diff)
	}

	if diff := pretty.Compare(pg.WALFileSize, 16777216); diff != "" {
		t.Fatalf("WALSegmentSize diff: (-got +want)\n%s", diff)
	}

	if diff := pretty.Compare(pg.WALFilesPerSegment, 256); diff != "" {
		t.Fatalf("WALSegmentsPerXLogId diff: (-got +want)\n%s", diff)
	}

	for n, test := range tests {
		test := test
		t.Run("", func(st *testing.T) {
			n := n
			st.Parallel()

			l, err := pg.ParseLSN(test.inLSN)
			if err != nil {
				st.Fatalf("bad: %v", err)
			}

			o := l.AddBytes(test.addBytes)
			if diff := pretty.Compare(test.outFilename, o.WALFilename(test.timeline)); diff != "" {
				st.Fatalf("%d: AddBytes diff: (-got +want)\n%s", n, diff)
			}
		})
	}
}

// Precompute the expected results from constants
func TestConstants(t *testing.T) {
	if diff := pretty.Compare(pg.WALPageSize, 8192); diff != "" {
		t.Fatalf("WALPageSize diff: (-got +want)\n%s", diff)
	}

	if diff := pretty.Compare(pg.WALFileSize, 16777216); diff != "" {
		t.Fatalf("WALSegmentSize diff: (-got +want)\n%s", diff)
	}

	if diff := pretty.Compare(pg.WALFilesPerSegment, 256); diff != "" {
		t.Fatalf("WALSegmentsPerXLogId diff: (-got +want)\n%s", diff)
	}
}

func TestLSN_Readahead(t *testing.T) {
	tests := []struct {
		inLSN       string
		timeline    pg.TimelineID
		filename    pg.WALFilename
		maxBytes    units.Base2Bytes
		outWALFiles []pg.WALFilename
	}{
		{
			inLSN:       "0/0",
			timeline:    100,
			filename:    "000000640000000000000000",
			maxBytes:    0,
			outWALFiles: []pg.WALFilename{},
		},
		{
			inLSN:       "0/1",
			timeline:    101,
			filename:    "000000650000000000000000",
			maxBytes:    pg.WALFileSize,
			outWALFiles: []pg.WALFilename{"000000650000000000000000"},
		},
		{
			inLSN:       "2/0",
			timeline:    102,
			filename:    "000000660000000200000000",
			maxBytes:    pg.WALFileSize,
			outWALFiles: []pg.WALFilename{"000000660000000200000000"},
		},
		{
			inLSN:       "3/4",
			timeline:    102,
			filename:    "000000660000000300000000",
			maxBytes:    pg.WALFileSize,
			outWALFiles: []pg.WALFilename{"000000660000000300000000"},
		},
		{
			inLSN:    "0/150E150",
			timeline: 10,
			filename: "0000000A0000000000000001",
			maxBytes: pg.WALFileSize + 1,
			outWALFiles: []pg.WALFilename{
				"0000000A0000000000000001",
				"0000000A0000000000000002",
			},
		},
		{
			inLSN:    "1/F50E150",
			timeline: 11,
			filename: "0000000B000000010000000F",
			maxBytes: 3 * pg.WALFileSize,
			outWALFiles: []pg.WALFilename{
				"0000000B000000010000000F",
				"0000000B0000000100000010",
				"0000000B0000000100000011",
			},
		},
		{
			inLSN:    "2/150E150",
			timeline: 12,
			filename: "0000000C0000000200000001",
			maxBytes: 2 * pg.WALFileSize,
			outWALFiles: []pg.WALFilename{
				"0000000C0000000200000001",
				"0000000C0000000200000002",
			},
		},
		{
			inLSN:    "ff/150E150",
			timeline: 13,
			filename: "0000000D000000FF00000001",
			maxBytes: 4 * pg.WALFileSize,
			outWALFiles: []pg.WALFilename{
				"0000000D000000FF00000001",
				"0000000D000000FF00000002",
				"0000000D000000FF00000003",
				"0000000D000000FF00000004",
			},
		},
	}

	for n, test := range tests {
		test := test
		t.Run("", func(st *testing.T) {
			n := n
			st.Parallel()

			l, err := pg.ParseLSN(test.inLSN)
			if err != nil {
				st.Fatalf("bad: %v", err)
			}

			if diff := pretty.Compare(test.filename, l.WALFilename(test.timeline)); diff != "" {
				st.Fatalf("%d: WALFilename diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.outWALFiles, l.Readahead(test.timeline, test.maxBytes)); diff != "" {
				st.Fatalf("%d: Readahead diff: (-got +want)\n%s", n, diff)
			}
		})
	}
}

func TestLSN_Type(t *testing.T) {
	tests := []struct {
		in       string
		out      string
		num      uint64
		timeline pg.TimelineID
		filename pg.WALFilename
		id       pg.HeapSegment
		offset   pg.Offset
		segment  pg.HeapSegment
	}{
		{
			in:       "0/150E150",
			out:      "0/150E150",
			num:      22077776,
			filename: "000000010000000000000001",
			id:       0,
			offset:   22077776,
			segment:  1,
		},
		{
			in:       "00/152A9C0",
			out:      "0/152A9C0",
			num:      22194624,
			timeline: 1,
			filename: "000000010000000000000001",
			id:       0,
			offset:   22194624,
			segment:  1,
		},
		{
			in:       "00/272E4558",
			out:      "0/272E4558",
			num:      657343832,
			filename: "000000010000000000000027",
			id:       0,
			offset:   657343832,
			segment:  39,
		},
		{
			in:       "FF/362E4558",
			out:      "FF/362E4558",
			num:      1096125662552,
			timeline: 0xff,
			filename: "000000FF000000FF00000036",
			id:       255,
			offset:   909002072,
			segment:  65334,
		},
	}

	for n, test := range tests {
		test := test
		t.Run("", func(st *testing.T) {
			n := n
			st.Parallel()

			l, err := pg.ParseLSN(test.in)
			if err != nil {
				st.Fatalf("bad: %v", err)
			}

			if diff := pretty.Compare(test.num, uint64(l)); diff != "" {
				st.Fatalf("%d: LSN diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.id, l.ID()); diff != "" {
				st.Fatalf("%d: ID diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.offset, l.ByteOffset()); diff != "" {
				st.Fatalf("%d: Offset diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.segment, l.SegmentNumber()); diff != "" {
				st.Fatalf("%d: Segment diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.out, l.String()); diff != "" {
				st.Fatalf("%d: String() diff: (-got +want)\n%s", n, diff)
			}

			// Test optional argument
			switch test.timeline {
			case 0:
				if diff := pretty.Compare(test.filename, l.WALFilename()); diff != "" {
					st.Fatalf("%d: WALFileName diff: (-got +want)\n%s", n, diff)
				}
			default:
				if diff := pretty.Compare(test.filename, l.WALFilename(test.timeline)); diff != "" {
					st.Fatalf("%d: WALFileName diff: (-got +want)\n%s", n, diff)
				}
			}
		})
	}
}
