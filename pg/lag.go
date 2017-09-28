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

package pg

import (
	"context"
	"math"

	"github.com/jackc/pgx"
)

const (
	// Value representing an invalid LSN (used in error conditions)
	InvalidLSN = LSN(math.MaxUint64)
)

// QueryOldestLSN queries the database to obtain the current TimelineID
// and either the current redo LSN (i.e. "apply lag LSN") or the replay LSN
// (recovery), whichever is older.
func QueryOldestLSN(ctx context.Context, pool *pgx.ConnPool) (TimelineID, LSN, error) {
	const (
		errTimelineID TimelineID = 0
	)

	const lagSQL = "SELECT timeline_id, redo_location, pg_last_xlog_replay_location() FROM pg_control_checkpoint()"
	rows, err := pool.QueryEx(ctx, lagSQL, nil)
	if err != nil {
		return errTimelineID, InvalidLSN, err
	}
	defer rows.Close()

	var timelineID TimelineID
	var redoLocation string
	var replayLocation *string
	for rows.Next() {
		err = rows.Scan(&timelineID, &redoLocation, &replayLocation)
		if err != nil {
			return errTimelineID, InvalidLSN, err
		}
	}

	if rows.Err() != nil {
		return errTimelineID, InvalidLSN, err
	}

	redoLSN, err := ParseLSN(redoLocation)
	if err != nil {
		return errTimelineID, InvalidLSN, err
	}

	var replayLSN LSN
	if replayLocation != nil {
		replayLSN, err = ParseLSN(*replayLocation)
		if err != nil {
			return errTimelineID, InvalidLSN, err
		}

		// If the replayLSN is newer than the redoLSN use the replayLSN because
		// we're likely in the midst of a recovery, therefore we want to take
		// prefaulting hints from whatever is an older LSN.
		if LSNCmp(redoLSN, replayLSN) < 1 {
			return timelineID, replayLSN, nil
		}
	}

	return timelineID, redoLSN, nil
}
