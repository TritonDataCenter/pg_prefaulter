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

package walcache

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestPGXLogDumpRE(t *testing.T) {
	tests := []struct {
		version      string
		input        []byte
		fail         bool
		tablespaceID []string
		databaseID   []string
		relationID   []string
		blockNumber  []string
	}{
		{
			version:      "pg9.6.3",
			input:        []byte(`rmgr: Heap        len (rec/tot):      3/    92, tx:       1851, lsn: B/E6000028, prev B/E5FFFFA0, desc: INSERT+INIT off 1, blkref #0: rel 1663/16398/16399 blk 4408314`),
			tablespaceID: []string{"1663"},
			databaseID:   []string{"16398"},
			relationID:   []string{"16399"},
			blockNumber:  []string{"4408314"},
		},
		{
			version:      "pg9.6.3+fork",
			input:        []byte(`rmgr: Heap2       len (rec/tot):      5/    59, tx:          0, lsn: 284/6273F620, prev 284/6273F5C8, desc: VISIBLE cutoff xid 1507571174 flags 1, blkref #0: rel 1663/16400/2619 fork vm blk 0, blkref #1: rel 1663/16400/2619 blk 10`),
			tablespaceID: []string{"1663", "1663"},
			databaseID:   []string{"16400", "16400"},
			relationID:   []string{"2619", "2619"},
			blockNumber:  []string{"0", "10"},
		},
		{
			version:      "pg9.6.3+split",
			input:        []byte(`rmgr: Btree       len (rec/tot):      8/  4406, tx: 1515267461, lsn: 287/5E0014C0, prev 287/5E001420, desc: SPLIT_L level 0, firstright 33, blkref #0: rel 1663/16400/16434 blk 9578854, blkref #1: rel 1663/16400/16434 blk 19938685, blkref #2: rel 1663/16400/16434 blk 3875203`),
			tablespaceID: []string{"1663", "1663", "1663"},
			databaseID:   []string{"16400", "16400", "16400"},
			relationID:   []string{"16434", "16434", "16434"},
			blockNumber:  []string{"9578854", "19938685", "3875203"},
		},
	}

	for i, test := range tests {
		xlogRE := pgXLogDumpRE.Copy()
		submatches := xlogRE.FindAllSubmatch(test.input, -1)
		if submatches == nil && !test.fail {
			t.Fatalf("%d failed to match test: %q", i, test.input)
		} else if test.fail {
			continue
		}

		if len(submatches) != len(test.tablespaceID) {
			t.Fatalf("%d failed to match the expected number of blocks: %q", i, test.input)
		}

		for j, submatch := range submatches {
			if len(submatch) != 5 {
				t.Fatalf("%d failed length test: %d", j, len(submatch))
			}

			if diff := pretty.Compare(string(submatch[1]), test.tablespaceID[j]); diff != "" {
				t.Fatalf("tablespace ID diff: (-got +want)\n%s", diff)
			}

			if diff := pretty.Compare(string(submatch[2]), test.databaseID[j]); diff != "" {
				t.Fatalf("database ID diff: (-got +want)\n%s", diff)
			}

			if diff := pretty.Compare(string(submatch[3]), test.relationID[j]); diff != "" {
				t.Fatalf("relation ID diff: (-got +want)\n%s", diff)
			}

			if diff := pretty.Compare(string(submatch[4]), test.blockNumber[j]); diff != "" {
				t.Fatalf("block number diff: (-got +want)\n%s", diff)
			}
		}
	}
}
