// Copyright © 2019 Joyent, Inc.
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

package pg

import (
	"fmt"
)

type WALTranslations struct {
	Major    uint64
	Directory  string
	Lsn        string
	Wal        string
	Queries    WALQueries
}

type WALQueries struct {
	OldestLSNs   string
	LagPrimary   string
	LagFollower  string
}

func Translate(pgMajor uint64) (WALTranslations) {
	var translations WALTranslations
	var translateHorizon uint64 = 100000 // PostgreSQL version 10

	var lagPrimaryFmt = `SELECT
	    state,
	    sync_state,
	    (pg_%[2]s_%[1]s_diff(sent_%[1]s, write_%[1]s))::FLOAT8 AS durability_lag_bytes,
	    (pg_%[2]s_%[1]s_diff(sent_%[1]s, flush_%[1]s))::FLOAT8 AS flush_lag_bytes,
	    (pg_%[2]s_%[1]s_diff(sent_%[1]s, replay_%[1]s))::FLOAT8 AS visibility_lag_bytes,
	    COALESCE(EXTRACT(EPOCH FROM '0'::INTERVAL), 0.0)::FLOAT8 AS visibility_lag_ms
	    FROM
	    pg_catalog.pg_stat_replication
	    ORDER BY visibility_lag_bytes
	    LIMIT 1`

	var lagFollowerFmt = `SELECT
	    'receiving' AS state,
	    'applying' AS sync_state,
	    0.0::FLOAT8 AS durability_lag_bytes,
	    0.0::FLOAT8 AS flush_lag_bytes,
	    COALESCE((pg_%[2]s_%[1]s_diff(pg_last_%[2]s_receive_%[1]s(), pg_last_%[2]s_replay_%[1]s()))::FLOAT8, 0.0)::FLOAT8 AS visibility_lag_bytes,
	    COALESCE(EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp())::INTERVAL), 0.0)::FLOAT8 AS visibility_lag_ms
	    LIMIT 1`

	translations = WALTranslations{}
	queries := WALQueries{}
	if pgMajor < translateHorizon {
		translations.Major = pgMajor
		translations.Directory = "pg_xlog"
		translations.Lsn = "location"
		translations.Wal = "xlog"
		queries.OldestLSNs = "SELECT timeline_id, redo_location, pg_last_xlog_replay_location() FROM pg_control_checkpoint()"
	} else {
		translations.Major = pgMajor
		translations.Directory = "pg_wal"
		translations.Lsn = "lsn"
		translations.Wal = "wal"
		queries.OldestLSNs = "SELECT timeline_id, redo_lsn, pg_last_wal_receive_lsn() FROM pg_control_checkpoint()"
	}

	queries.LagPrimary = fmt.Sprintf(lagPrimaryFmt, translations.Lsn, translations.Wal)
	queries.LagFollower = fmt.Sprintf(lagFollowerFmt, translations.Lsn, translations.Wal)

	translations.Queries = queries

	return translations
}
