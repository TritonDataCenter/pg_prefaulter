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

package agent

import (
	"github.com/joyent/pg_prefaulter/agent/proc"
	"github.com/joyent/pg_prefaulter/pg"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// getWALFilesProcArgs finds the PostgreSQL parent PID and looks through all
// processes that decend from PostgreSQL to parse out the current WAL file
// contained in the args.
func (a *Agent) getWALFilesProcArgs() (walFiles pg.WALFiles, err error) {
	parentPid, err := a.findPostgreSQLPostmasterPID()
	if err != nil {
		return nil, errors.Wrap(err, "unable to find the PostgreSQL pid")
	}

	childPIDs, err := proc.FindChildPIDs(a.shutdownCtx, parentPid)
	if err != nil {
		return nil, errors.Wrap(err, "unable to find any PostgreSQL child processes")
	}

	walFile, err := proc.FindWALFileFromPIDArgs(a.shutdownCtx, childPIDs, a.metrics)
	if err != nil {
		return nil, errors.Wrap(err, "unable to find a WAL file from pids")
	}

	walFiles, err = a.predictProcWALFilenames(walFile)
	if err != nil {
		log.Debug().Err(err).Msg("unable to predict proc WAL filenames")
		return walFiles, err
	}

	return walFiles, nil
}

// predictProcWALFilenames guesses what the filenames are going to be in advance
// of PostgreSQL naively processing a WAL file.  Use walFile as the seed
// filename to indicate where we are in the WAL stream and forecast N WAL
// filenames where N is the configured level of WAL readahead.  Errors are
// logged, but the return will always include at least one WAL file.
//
// Unlike predictDBWALFilenames(), predictProcWALFilenames() uses a derived LSN
// from the WAL filename to predict the next WAL segment to process (as opposed
// to querying the database and potentially backing off).
func (a *Agent) predictProcWALFilenames(walFile pg.WALFilename) (pg.WALFiles, error) {
	timelineID, walLSN, err := pg.ParseWalfile(walFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse the WAL filename")
	}

	// Clamp the number of bytes we'll readahead in order to prevent reading into
	// the future.
	maxBytes := a.walCache.ReadaheadBytes()

	return walLSN.Readahead(timelineID, maxBytes), nil
}
