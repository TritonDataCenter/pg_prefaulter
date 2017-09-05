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
