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

package fhcache

import (
	"fmt"
	"path"
	"strconv"

	"github.com/joyent/pg_prefaulter/agent/structs"
	"github.com/joyent/pg_prefaulter/pg"
)

// _Key is a comparable forward lookup key.  These values almost certainly need
// to be kept in sync with structs.IOCacheKey, however the FileHandleCache only
// needs the segment, not the block number.
type _Key struct {
	tablespace pg.OID
	database   pg.OID
	relation   pg.OID
	segment    pg.HeapSegmentNumber
}

func _NewKey(ioCacheKey structs.IOCacheKey) _Key {
	return _Key{
		tablespace: ioCacheKey.Tablespace,
		database:   ioCacheKey.Database,
		relation:   ioCacheKey.Relation,
		segment:    ioCacheKey.Block.SegmentNumber(),
	}
}

// filename generates the absolute path filename for a given _Key.
func (key *_Key) filename(pgdataPath string) string {
	// FIXME(seanc@): Move this logic to the pg package.  Create an "LSN"
	// interface that requires the necessary helper functions so that a
	// fhcache.Key can be used to pg.* methods.
	var filename string
	if key.segment > 0 {
		// It's easier to abuse Relation here than to support a parallel refilno
		// struct member
		filename = fmt.Sprintf("%d.%d", key.relation, key.segment)
	} else {
		filename = strconv.FormatUint(uint64(key.relation), 10)
	}

	filename = path.Join(pgdataPath, "base", strconv.FormatUint(uint64(key.database), 10), string(filename))

	return filename
}
