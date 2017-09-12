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

package structs

import "github.com/joyent/pg_prefaulter/pg"

// IOCacheKey contains the forward lookup information for a given relation file.
// IOCacheKey is a
// [comparable](https://golang.org/ref/spec#Comparison_operators) struct
// suitable for use as a lookup key.  These values are immutable and map 1:1
// with the string inputs read from the pg_xlogdump(1) scanning utility.
type IOCacheKey struct {
	Tablespace pg.OID
	Database   pg.OID
	Relation   pg.OID
	Block      pg.HeapBlockNumber
}
