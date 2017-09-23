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

	tests := []struct {
	}{
// Test adding bytes values to an LSN
func TestLSN_AddBytes(t *testing.T) {
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
			inLSN:       "1/0",
			addBytes:    pg.WALFileSize,
			equalLSN:    "1/1000000",
			timeline:    101,
			outFilename: "000000650000000100000001",
		},
		{
			inLSN:       "2/1",
			addBytes:    2 * pg.WALFileSize,
			equalLSN:    "2/2000001",
			timeline:    102,
			outFilename: "000000660000000200000002",
		},
		{
			inLSN:       "FF/FF",
			addBytes:    3*pg.WALFileSize + 2,
			equalLSN:    "FF/3000101",
			timeline:    103,
			outFilename: "00000067000000FF00000003",
		},
	}

	for n, test := range tests {
		test := test
		t.Run("", func(st *testing.T) {
			n := n
			st.Parallel()

			lsn, err := pg.ParseLSN(test.inLSN)
			if err != nil {
				st.Fatalf("bad: %v", err)
			}

			o := lsn.AddBytes(test.addBytes)
			if diff := pretty.Compare(test.equalLSN, o.String()); diff != "" {
				st.Errorf("%d: add result diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.outFilename, o.WALFilename(test.timeline)); diff != "" {
				st.Errorf("%d: AddBytes diff: (-got +want)\n%s", n, diff)
			}
		})
	}
}

func TestLSN_ParseWALFilename(t *testing.T) {
	// NOTE(seanc@): All outLSN values have a 0 byte offset in order to be able to
	// roundtrip the LSN to a WAL filename.
	tests := []struct {
		inFilename    pg.WALFilename
		outLSN        string
		outTimelineID pg.TimelineID
	}{
		{
			inFilename:    "000000640000000000000000",
			outTimelineID: 100,
			outLSN:        "0/0",
		},
		{
			inFilename:    "000000650000000000000000",
			outTimelineID: 101,
			outLSN:        "0/0",
		},
		{
			inFilename:    "000000660000000200000000",
			outTimelineID: 102,
			outLSN:        "2/0",
		},
		{
			inFilename:    "000000660000000300000000",
			outTimelineID: 102,
			outLSN:        "3/0",
		},
		{
			inFilename:    "0000000A0000000000000000",
			outTimelineID: 10,
			outLSN:        "0/0",
		},
		{
			inFilename:    "0000000B0000000100000000",
			outTimelineID: 11,
			outLSN:        "1/0",
		},
		{
			inFilename:    "0000000C00000002000000FF",
			outTimelineID: 12,
			outLSN:        "2/FF",
		},
		{
			inFilename:    "0000000D0000FF00FFFFFFFF",
			outTimelineID: 13,
			outLSN:        "FF00/FFFFFFFF",
		},
	}

	for n, test := range tests {
		test := test
		t.Run("", func(st *testing.T) {
			n := n
			st.Parallel()

			tid, lsn, err := pg.ParseWalfile(test.inFilename)
			if err != nil {
				st.Fatalf("bad: %v", err)
			}

			if diff := pretty.Compare(test.outTimelineID, tid); diff != "" {
				st.Errorf("%d: ParseWalfile TID diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.outLSN, lsn.String()); diff != "" {
				st.Errorf("%d: ParseWalfile LSN round trip diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.inFilename, lsn.WALFilename(tid)); diff != "" {
				st.Errorf("%d: ParseWalfile WAL Filename diff: (-got +want)\n%s", n, diff)
			}
		})
	}
}

func TestLSN_Readahead(t *testing.T) {
	tests := []struct {
		lsn         string
		timeline    pg.TimelineID
		filename    pg.WALFilename
		maxBytes    units.Base2Bytes
		outWALFiles []pg.WALFilename
	}{
		{
			lsn:         "0/0",
			timeline:    100,
			filename:    "000000640000000000000000",
			maxBytes:    0,
			outWALFiles: []pg.WALFilename{},
		},
		{
			lsn:         "0/1",
			timeline:    101,
			filename:    "000000650000000000000000",
			maxBytes:    pg.WALFileSize,
			outWALFiles: []pg.WALFilename{"000000650000000000000000"},
		},
		{
			lsn:         "2/0",
			timeline:    102,
			filename:    "000000660000000200000000",
			maxBytes:    pg.WALFileSize,
			outWALFiles: []pg.WALFilename{"000000660000000200000000"},
		},
		{
			lsn:         "3/4",
			timeline:    102,
			filename:    "000000660000000300000000",
			maxBytes:    pg.WALFileSize,
			outWALFiles: []pg.WALFilename{"000000660000000300000000"},
		},
		{
			lsn:      "0/150E150",
			timeline: 10,
			filename: "0000000A0000000000000001",
			maxBytes: pg.WALFileSize + 1,
			outWALFiles: []pg.WALFilename{
				"0000000A0000000000000001",
				"0000000A0000000000000002",
			},
		},
		{
			lsn:      "1/F50E150",
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
			lsn:      "2/150E150",
			timeline: 12,
			filename: "0000000C0000000200000001",
			maxBytes: 2 * pg.WALFileSize,
			outWALFiles: []pg.WALFilename{
				"0000000C0000000200000001",
				"0000000C0000000200000002",
			},
		},
		{
			lsn:      "ff/150E150",
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

			lsn, err := pg.ParseLSN(test.lsn)
			if err != nil {
				st.Fatalf("bad: %v", err)
			}

			if diff := pretty.Compare(test.filename, lsn.WALFilename(test.timeline)); diff != "" {
				st.Errorf("%d: WALFilename diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.outWALFiles, lsn.Readahead(test.timeline, test.maxBytes)); diff != "" {
				st.Errorf("%d: Readahead diff: (-got +want)\n%s", n, diff)
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

			lsn, err := pg.ParseLSN(test.in)
			if err != nil {
				st.Fatalf("bad: %v", err)
			}

			if diff := pretty.Compare(test.num, uint64(lsn)); diff != "" {
				st.Errorf("%d: LSN diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.id, lsn.ID()); diff != "" {
				st.Errorf("%d: ID diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.offset, lsn.ByteOffset()); diff != "" {
				st.Errorf("%d: Offset diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.segment, lsn.SegmentNumber()); diff != "" {
				st.Errorf("%d: Segment diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.out, lsn.String()); diff != "" {
				st.Errorf("%d: String() diff: (-got +want)\n%s", n, diff)
			}

			// Test optional argument
			switch test.timeline {
			case 0:
				if diff := pretty.Compare(test.filename, lsn.WALFilename()); diff != "" {
					st.Errorf("%d: WALFileName diff: (-got +want)\n%s", n, diff)
				}
			default:
				if diff := pretty.Compare(test.filename, lsn.WALFilename(test.timeline)); diff != "" {
					st.Errorf("%d: WALFileName diff: (-got +want)\n%s", n, diff)
				}
			}
		})
	}
}
func TestLSN_Cmp(t *testing.T) {
	tests := []struct {
		x   pg.LSN
		y   pg.LSN
		ret int
	}{
		// Test bytes
		{ // 0
			x:   pg.MustParseLSN("1/0"),
			y:   pg.MustParseLSN("1/0"),
			ret: 0,
		},
		{ // 1
			x:   pg.MustParseLSN("1/0"),
			y:   pg.MustParseLSN("1/1"),
			ret: -1,
		},
		{ // 2
			x:   pg.MustParseLSN("1/1"),
			y:   pg.MustParseLSN("1/0"),
			ret: 1,
		},
		// Test segment IDs
		{ // 3
			x:   pg.MustParseLSN("FA/0"),
			y:   pg.MustParseLSN("FA/0"),
			ret: 0,
		},
		{ // 4
			x:   pg.MustParseLSN("FB/0"),
			y:   pg.MustParseLSN("FC/0"),
			ret: -1,
		},
		{ // 5
			x:   pg.MustParseLSN("FD/0"),
			y:   pg.MustParseLSN("F8/0"),
			ret: 1,
		},
		// Test values from pg_lsn
		{ // 6
			x:   pg.MustParseLSN("0/16AE7F8"),
			y:   pg.MustParseLSN("0/16AE7F8"),
			ret: 0,
		},
		{ // 7
			x:   pg.MustParseLSN("0/16AE7F7"),
			y:   pg.MustParseLSN("0/16AE7F8"),
			ret: -1,
		},
		{ // 8
			x:   pg.MustParseLSN("0/16AE7F8"),
			y:   pg.MustParseLSN("0/16AE7F7"),
			ret: 1,
		},
	}

	for n, test := range tests {
		test := test
		t.Run("", func(st *testing.T) {
			n := n
			st.Parallel()

			if diff := pretty.Compare(test.ret, pg.LSNCmp(test.x, test.y)); diff != "" {
				st.Logf("%d: x %+q y %+q", n, test.x, test.y)
				st.Errorf("%d: LSNCmp diff: (-got +want)\n%s", n, diff)
			}
		})
	}
}

