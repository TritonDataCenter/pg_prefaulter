package lsn_test

import (
	"testing"

	"github.com/joyent/pg_prefaulter/lsn"
	"github.com/kylelemons/godebug/pretty"
)

func TestType(t *testing.T) {
	tests := []struct {
		num    uint64
		in     string
		out    string
		id     uint32
		offset uint32
	}{
		{
			num:    22077776,
			in:     "0/150E150",
			out:    "0/150E150",
			id:     0,
			offset: 22077776,
		},
		{
			num:    22194624,
			in:     "00/152A9C0",
			out:    "0/152A9C0",
			id:     0,
			offset: 22194624,
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

			if diff := pretty.Compare(test.out, l.String()); diff != "" {
				st.Fatalf("%d: String() diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.id, l.ID()); diff != "" {
				st.Fatalf("%d: ID diff: (-got +want)\n%s", n, diff)
			}

			if diff := pretty.Compare(test.offset, l.Offset()); diff != "" {
				st.Fatalf("%d: Offset diff: (-got +want)\n%s", n, diff)
			}
		})
	}
}
