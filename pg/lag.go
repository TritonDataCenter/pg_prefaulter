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

	"github.com/jackc/pgx"
)

// QueryWALLogFile queries the database to obtain the most recent WAL log segment
// and TimelineID.
func QueryWALLogFile(ctx context.Context, pool *pgx.ConnPool) (TimelineID, WALFilename, error) {
	const (
		errTimelineID  TimelineID  = 0
		errWALFilename WALFilename = ""
	)

	const lagSQL = "SELECT timeline_id, redo_wal_file FROM pg_control_checkpoint()"
	rows, err := pool.QueryEx(ctx, lagSQL, nil)
	if err != nil {
		return errTimelineID, errWALFilename, err
	}
	defer rows.Close()

	var timelineID TimelineID
	var walFile WALFilename
	for rows.Next() {
		err = rows.Scan(&timelineID, &walFile)
		if err != nil {
			return errTimelineID, errWALFilename, err
		}
	}

	if rows.Err() != nil {
		return errTimelineID, errWALFilename, err
	}

	return timelineID, walFile, nil
}
