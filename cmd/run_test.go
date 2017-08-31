package cmd

import (
	"testing"

	"github.com/y0ssar1an/q"
)

func TestXLogdumpRE(t *testing.T) {
	var line []byte = []byte(`[cur:FC/D9000178, xid:567408626, rmid:11(Btree), len/tot_len:130/162, info:0, prev:FC/D8FFFE50] insert_leaf: s/d/r:1663/16385/16419 tid 3390809/49`)
	xlogRE := xlogdumpRE.Copy()

	submatches := xlogRE.FindAllSubmatch(line, -1)

	q.Q(string(line), submatches)
}

func TestPGXLogDumpRE(t *testing.T) {
	var line []byte = []byte(`[cur:FC/D9000178, xid:567408626, rmid:11(Btree), len/tot_len:130/162, info:0, prev:FC/D8FFFE50] insert_leaf: s/d/r:1663/16385/16419 tid 3390809/49`)
	xlogRE := xlogdumpRE.Copy()

	submatches := xlogRE.FindAllSubmatch(line, -1)

	q.Q(string(line), submatches)
}
