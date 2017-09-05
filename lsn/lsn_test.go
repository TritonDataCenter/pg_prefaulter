package lsn_test

import (
	"testing"

	"github.com/joyent/pg_prefaulter/lsn"
	"github.com/kylelemons/godebug/pretty"
)

// Precompute the expected results from constants
func TestConstants(t *testing.T) {
	if diff := pretty.Compare(lsn.WALPageSize, 8192); diff != "" {
		t.Fatalf("WALPageSize diff: (-got +want)\n%s", diff)
	}

	if diff := pretty.Compare(lsn.WALSegmentSize, 16777216); diff != "" {
		t.Fatalf("WALSegmentSize diff: (-got +want)\n%s", diff)
	}

	if diff := pretty.Compare(lsn.WALSegmentsPerXLogId, 256); diff != "" {
		t.Fatalf("WALSegmentsPerXLogId diff: (-got +want)\n%s", diff)
	}
}

func TestType(t *testing.T) {
	tests := []struct {
		in       string
		out      string
		num      uint64
		timeline lsn.TimelineID
		filename string
		id       uint32
		offset   uint32
		segment  uint32
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
			st.Parallel()

			l, err := lsn.Parse(test.in)
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
				if diff := pretty.Compare(test.filename, l.WALFileName()); diff != "" {
					st.Fatalf("%d: WALFileName diff: (-got +want)\n%s", n, diff)
				}
			default:
				if diff := pretty.Compare(test.filename, l.WALFileName(test.timeline)); diff != "" {
					st.Fatalf("%d: WALFileName diff: (-got +want)\n%s", n, diff)
				}
			}
		})
	}
}
