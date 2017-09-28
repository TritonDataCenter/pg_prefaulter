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

func TestLSN(t *testing.T) {
	tests := []struct {
		inLSNString      string
		wantFail         bool
		inTimelineID     pg.TimelineID
		outSegmentNumber pg.WALSegmentNumber
		outSegmentHigh   pg.WALSegmentNumber
		outSegmentLow    pg.WALSegmentNumber
		outByteOffset    pg.WALByteOffset
		outString        string
		outUint64        uint64
		outWALFilename   pg.WALFilename
	}{
		{ // 0
			inLSNString:      "0/150E150",
			outString:        "0/150E150",
			outUint64:        0x150E150,
			inTimelineID:     1,
			outWALFilename:   "000000010000000000000001",
			outByteOffset:    5300560,
			outSegmentNumber: 1,
			outSegmentHigh:   0x00000000,
			outSegmentLow:    0x00000001,
		},
		{ // 1
			inLSNString:      "00/152A9C0",
			outString:        "0/152A9C0",
			outUint64:        0x152A9C0,
			inTimelineID:     2,
			outWALFilename:   "000000020000000000000001",
			outByteOffset:    5417408,
			outSegmentNumber: 1,
			outSegmentHigh:   0x00000000,
			outSegmentLow:    0x00000001,
		},
		{ // 2
			inLSNString:      "02/372E4558",
			inTimelineID:     3,
			outString:        "2/372E4558",
			outUint64:        0x2372E4558,
			outWALFilename:   "000000030000000200000037",
			outByteOffset:    0x2E4558,
			outSegmentNumber: 567,
			outSegmentHigh:   0x00000002,
			outSegmentLow:    0x00000037,
		},
		{ // 3
			inLSNString:      "FFFF/462E4558",
			inTimelineID:     0xff,
			outString:        "FFFF/462E4558",
			outUint64:        0xFFFF462E4558,
			outWALFilename:   "000000FF0000FFFF00000046",
			outByteOffset:    3032408,
			outSegmentNumber: 16777030,
			outSegmentHigh:   0x0000FFFF,
			outSegmentLow:    0x00000046,
		},
		// Begin regression values from pg_lsn
		{ // 4
			inLSNString:      "FFFFFFFF/FFFFFFFF",
			inTimelineID:     0xEE00,
			outString:        "FFFFFFFF/FFFFFFFF",
			outUint64:        0xFFFFFFFFFFFFFFFF,
			outWALFilename:   "0000EE00FFFFFFFF000000FF",
			outByteOffset:    16777215,
			outSegmentNumber: 0xFFFFFFFFFF,
			outSegmentHigh:   0xFFFFFFFF,
			outSegmentLow:    0x000000FF,
		},
		{ // 5
			inLSNString: "G/0",
			wantFail:    true,
		},
		{ // 6
			inLSNString: "-1/0",
			wantFail:    true,
		},
		{ // 7
			inLSNString: " 0/12345678",
			wantFail:    true,
		},
		{ // 8
			inLSNString: "ABCD/",
			wantFail:    true,
		},
		{ // 9
			inLSNString: "/ABCD",
			wantFail:    true,
		},
	}

	for n, test := range tests {
		test := test
		t.Run("", func(st *testing.T) {
			n := n
			st.Parallel()

			lsn, err := pg.ParseLSN(test.inLSNString)
			switch {
			case err != nil && test.wantFail:
				return
			case err != nil && !test.wantFail:
				st.Fatalf("bad: %v", err)
			case err == nil && test.wantFail:
				st.Fatalf("bad test %d, expected failure for %+q", n, test.inLSNString)
			case err == nil && !test.wantFail:
				// fall through
			}

			if diff := pretty.Compare(lsn.ByteOffset(), test.outByteOffset); diff != "" {
				st.Errorf("%d: Offset diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(lsn.SegmentNumber(), test.outSegmentNumber); diff != "" {
				st.Errorf("%d: SegmentNumber diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(lsn.SegmentNumber().High(), test.outSegmentHigh); diff != "" {
				st.Errorf("%d: SegmentNumber High bits diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(lsn.SegmentNumber().Low(), test.outSegmentLow); diff != "" {
				st.Errorf("%d: SegmentNumber Low bits diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(lsn.String(), test.outString); diff != "" {
				st.Errorf("%d: String() diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(uint64(lsn), test.outUint64); diff != "" {
				st.Errorf("%d: LSN diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(lsn.WALFilename(test.inTimelineID), test.outWALFilename); diff != "" {
				st.Errorf("%d: WALFileName diff: (-got +want)\n%s", n, diff)
			}
		})
	}
}

// Test adding bytes values to an LSN
func TestLSN_AddBytes(t *testing.T) {
	tests := []struct {
		inLSN    pg.LSN
		addBytes units.Base2Bytes
		equalLSN pg.LSN
	}{
		{ // 0
			inLSN:    pg.MustParseLSN("0/0"),
			addBytes: 0,
			equalLSN: pg.MustParseLSN("0/0"),
		},
		{ // 1
			inLSN:    pg.MustParseLSN("1/0"),
			addBytes: pg.WALSegmentSize,
			equalLSN: pg.MustParseLSN("1/1000000"),
		},
		{ // 2
			inLSN:    pg.MustParseLSN("2/1"),
			addBytes: 2 * pg.WALSegmentSize,
			equalLSN: pg.MustParseLSN("2/2000001"),
		},
		{ // 3
			inLSN:    pg.MustParseLSN("FF/FF"),
			addBytes: 3*pg.WALSegmentSize + 2,
			equalLSN: pg.MustParseLSN("FF/3000101"),
		},
	}

	for n, test := range tests {
		test := test
		t.Run("", func(st *testing.T) {
			n := n
			st.Parallel()

			o := test.inLSN.AddBytes(test.addBytes)
			if diff := pretty.Compare(o, test.equalLSN); diff != "" {
				st.Errorf("%d: add result diff: (-got +want)\n%s", n, diff)
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

			if diff := pretty.Compare(pg.LSNCmp(test.x, test.y), test.ret); diff != "" {
				st.Errorf("%d: LSNCmp diff: (-got +want)\n%s", n, diff)
			}
		})
	}
}

func TestLSN_ParseWALFilename(t *testing.T) {
	// NOTE(seanc@): All outLSN values have a 0 byte offset in order to be able to
	// roundtrip the LSN to a WAL filename.
	tests := []struct {
		inWALFilename pg.WALFilename
		outLSN        pg.LSN
		outTimelineID pg.TimelineID
	}{
		{ // 0
			inWALFilename: "000000640000000100000000",
			outTimelineID: 100,
			outLSN:        pg.MustParseLSN("1/1"),
		},
		{ // 1
			inWALFilename: "00000065000000FF00000000",
			outTimelineID: 101,
			outLSN:        pg.MustParseLSN("FF/1"),
		},
		{ // 2
			inWALFilename: "0000000A0000000D0000004F",
			outTimelineID: 10,
			outLSN:        pg.MustParseLSN("D/4F000001"),
		},
		{ // 3
			inWALFilename: "000000660100000000000000",
			outTimelineID: 102,
			outLSN:        pg.MustParseLSN("1000000/1"),
		},
		{ // 4
			inWALFilename: "000000660000000200000000",
			outTimelineID: 102,
			outLSN:        pg.MustParseLSN("2/1"),
		},
		{ // 5
			inWALFilename: "0000000A0000000000000000",
			outTimelineID: 10,
			outLSN:        pg.MustParseLSN("0/1"),
		},
		{ // 6
			inWALFilename: "0000000B0000000300000000",
			outTimelineID: 11,
			outLSN:        pg.MustParseLSN("3/1"),
		},
		{ // 7
			inWALFilename: "0000000C0000000400000000",
			outTimelineID: 12,
			outLSN:        pg.MustParseLSN("4/1"),
		},
		{ // 8
			inWALFilename: "0000000D0000FF00000000FF",
			outTimelineID: 13,
			outLSN:        pg.MustParseLSN("FF00/FF000001"),
		},
	}

	for n, test := range tests {
		test := test
		t.Run("", func(st *testing.T) {
			n := n
			st.Parallel()

			tid, lsn, err := pg.ParseWalfile(test.inWALFilename)
			if err != nil {
				st.Fatalf("bad: %v", err)
			}

			if diff := pretty.Compare(tid, test.outTimelineID); diff != "" {
				st.Errorf("%d: ParseWalfile TimelineID diff: (-got +want)\n%s", n, diff)
			}

			// Prevent chasing our tails with bad test input
			if test.outLSN.ByteOffset() != 1 {
				st.Fatalf("%d: bad LSN input (can't have a zero byte offset, use 1): %v", n, test.outLSN)
			}

			// The inputs and outputs are not naively isomorphic (i.e. can't
			// round-trip the input to output and back).
			if diff := pretty.Compare(lsn, test.outLSN); diff != "" {
				st.Errorf("%d: ParseWalfile LSN round trip diff: (-got +want)\n%s", n, diff)
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
		{ // 0
			lsn:         "0/1",
			timeline:    100,
			filename:    "000000640000000000000000",
			maxBytes:    0,
			outWALFiles: []pg.WALFilename{},
		},
		{ // 1
			lsn:         "0/1",
			timeline:    101,
			filename:    "000000650000000000000000",
			maxBytes:    pg.WALSegmentSize,
			outWALFiles: []pg.WALFilename{"000000650000000000000000"},
		},
		{ // 2
			lsn:         "2/1",
			timeline:    102,
			filename:    "000000660000000200000000",
			maxBytes:    pg.WALSegmentSize,
			outWALFiles: []pg.WALFilename{"000000660000000200000000"},
		},
		{ // 3
			lsn:         "3/4",
			timeline:    102,
			filename:    "000000660000000300000000",
			maxBytes:    pg.WALSegmentSize,
			outWALFiles: []pg.WALFilename{"000000660000000300000000"},
		},
		{ // 4
			lsn:      "0/150E150",
			timeline: 10,
			filename: "0000000A0000000000000001",
			maxBytes: pg.WALSegmentSize + 1,
			outWALFiles: []pg.WALFilename{
				"0000000A0000000000000001",
				"0000000A0000000000000002",
			},
		},
		{ // 5
			lsn:      "1/F50E150",
			timeline: 11,
			filename: "0000000B000000010000000F",
			maxBytes: 3 * pg.WALSegmentSize,
			outWALFiles: []pg.WALFilename{
				"0000000B000000010000000F",
				"0000000B0000000100000010",
				"0000000B0000000100000011",
			},
		},
		{ // 6
			lsn:      "2/150E150",
			timeline: 12,
			filename: "0000000C0000000200000001",
			maxBytes: 2 * pg.WALSegmentSize,
			outWALFiles: []pg.WALFilename{
				"0000000C0000000200000001",
				"0000000C0000000200000002",
			},
		},
		{ // 7
			lsn:      "ff/150E150",
			timeline: 13,
			filename: "0000000D000000FF00000001",
			maxBytes: 4 * pg.WALSegmentSize,
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

			// NOTE(seanc@): we are *NOT* round-tripping the LSN to WAL file and back,
			// instead we are just testing the WAL file, therefore there is no guard
			// to prevent an invalid byte offset of 0 from being used in the test
			// inputs.

			if diff := pretty.Compare(lsn.WALFilename(test.timeline), test.filename); diff != "" {
				st.Errorf("%d: WALFilename diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(lsn.Readahead(test.timeline, test.maxBytes), test.outWALFiles); diff != "" {
				st.Errorf("%d: Readahead diff: (-got +want)\n%s", n, diff)
			}
		})
	}
}
