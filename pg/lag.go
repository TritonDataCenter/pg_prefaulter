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
	"context"
	"math"

	"github.com/jackc/pgx"
	"github.com/rs/zerolog/log"
)

const (
	// Value representing an invalid LSN (used in error conditions)
	InvalidLSN = LSN(math.MaxUint64)

	// NumOldLSNs is the number of LSNs extracted from QueryOldestLSNs()
	NumOldLSNs = 2
)

// QueryOldestLSNs queries the database to obtain the current TimelineID and the
// oldest LSNs that it is processing.
func QueryOldestLSNs(ctx context.Context, pool *pgx.ConnPool, inProcess WALStatusChecker, walTranslations *WALTranslations) (TimelineID, []LSN, error) {
	const (
		errTimelineID TimelineID = 0
	)

	// The following is a useful debugging query:
	//
	//    SELECT checkpoint_location, checkpoint_location, redo_location,
	//    redo_wal_file, pg_last_xlog_receive_location(),
	//    pg_last_xlog_replay_location() FROM pg_control_checkpoint();

	// NOTE(seanc@): keep the number of LSN values in this query with
	// the NumOldLSNs constant.
	rows, err := pool.QueryEx(ctx, walTranslations.Queries.OldestLSNs, nil)
	if err != nil {
		return errTimelineID, nil, err
	}
	defer rows.Close()

	var timelineID TimelineID
	var redoLocation string
	var replayLocation *string
	for rows.Next() {
		err = rows.Scan(&timelineID, &redoLocation, &replayLocation)
		if err != nil {
			return errTimelineID, nil, err
		}
	}

	if rows.Err() != nil {
		return errTimelineID, nil, err
	}

	oldLSNs := make([]LSN, 0, NumOldLSNs)
	{
		redoLSN, err := ParseLSN(redoLocation)
		if err != nil {
			return errTimelineID, nil, err
		}
		oldLSNs = append(oldLSNs, redoLSN)

		redoWALFile := redoLSN.WALFilename(timelineID)
		if !inProcess.InProcess(redoWALFile) {
			log.Debug().Str("walfile", string(redoWALFile)).
				Str("type", "redo").
				Msg("found redo WAL segment from DB")
		}
	}

	{
		if replayLocation != nil {
			replayLSN, err := ParseLSN(*replayLocation)
			if err != nil {
				return errTimelineID, nil, err
			}
			oldLSNs = append(oldLSNs, replayLSN)

			replayWALFile := replayLSN.WALFilename(timelineID)
			if !inProcess.InProcess(replayWALFile) {
				log.Debug().Str("walfile", string(replayWALFile)).
					Str("type", "replay").
					Msg("found replay WAL segment from DB")
			}
		}
	}

	return timelineID, oldLSNs, nil
}
