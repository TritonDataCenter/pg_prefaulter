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
	"github.com/bluele/gcache"
	"github.com/joyent/pg_prefaulter/lsn"
	log "github.com/rs/zerolog/log"
)

// runFollower is excuted when talking to a readonly follower.  When returning
// true, we're requesting an immediately loop without any pause between
// iterations.
func (a *Agent) runFollower() (loopImmediately bool) {
	visibilityLagBytes, err := a.queryLag(_QueryLagFollower)
	if err != nil {
		log.Error().Err(err).Msg("unable to query follower lag")
		return false
	}

	var timelineID lsn.TimelineID
	timelineID, err = a.queryLastLog()
	if err != nil {
		log.Error().Err(err).Msg("unable to query last WAL lag")
		return false
	}

	replayLSN, err := a.queryLSN(LastXLogReplayLocation)
	if err != nil {
		log.Error().Err(err).Msg("unable to query LSN")
		return false
	}

	// Precalculate the WAL files we need to proactively fault in based on the
	// timeline and LSN.  Don't read into the future.
	maxBytes := uint64(a.walReadAhead * uint32(lsn.WALFileSize))
	if maxBytes > visibilityLagBytes {
		maxBytes = visibilityLagBytes
	}
	walFiles := make([]string, 0, a.walReadAhead)
	for i := uint32(0); i < a.walReadAhead; i++ {
		segNo := replayLSN.ID()
		off := replayLSN.ByteOffset()
		if uint64(off)+maxBytes <= uint64(off)+(uint64(i+1)*uint64(lsn.WALFileSize))-1 {
			// log.Debug().Int("segno", int(segNo)).Int("off", int(off)).Int("max bytes", int(maxBytes)).Int("vis lag", int(visibilityLagBytes)).Msg("run follower wal loop break")
			break
		}
		l := lsn.New(segNo, lsn.Offset(uint32(off)+(i*uint32(lsn.WALFileSize))))
		walFiles = append(walFiles, l.WALFileName(timelineID))
	}

	// 1) Read through the cache to prefault a given WAL file.  The cache lies to
	//    us and begins faulting the WAL file as soon as we request it.  Requests
	//    are deduped and the cache is in place in order to prevent a WAL file
	//    from being prefaulted a second time.
	// 2) Perform all cache lookups using GetIFPresent() in order to trigger a
	//    backfill of the entry.  GetIFPresent() has a side-effect of launching
	//    the LoaderFunc(), which will populate the cache and deduplicate requests
	//    if the cache hasn't been filled by the subsequent iteration through the
	//    cache.  If all entries were found in the cache, sleep.  If we had any
	//    cache misses loop immediately.
	for _, walFile := range walFiles {
		_, err := a.walCache.GetIFPresent(walFile)
		if err == gcache.KeyNotFoundError {
			loopImmediately = true
		}
	}

	// If we had a single cache miss previously, perform the exact same lookups a
	// second time, but this time with a blocking call to Get().  We perform this
	// second loop through the cache in order to limit the amount of activity and
	// let the dispatched work run to completion before attempting to process
	// additional WAL files.
	if loopImmediately {
		for _, walFile := range walFiles {
			if _, err := a.walCache.Get(walFile); err != nil {
				log.Error().Err(err).Msg("unable to perform synchronous Get operation on WAL file cache")
			}
		}
	}

	// log.Debug().Bool("loop", loopImmediately).Str("current wal-file", replayLSN.WALFileName(timelineID)).Strs("wal files", walFiles).Msg("")

	return loopImmediately
}
