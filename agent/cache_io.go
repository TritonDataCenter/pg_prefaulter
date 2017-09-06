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
	"io"
	"strconv"
	"time"

	"github.com/bluele/gcache"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/joyent/pg_prefaulter/lsn"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
)

// _IOCacheKey contains the forward lookup information for a given relation
// file.  _IOCacheKey is a
// [comparable](https://golang.org/ref/spec#Comparison_operators) struct
// suitable for use as a lookup key.  These values are immutable and map 1:1
// with the string inputs read from the pg_xlogdump(1) scanning utility.
type _IOCacheKey struct {
	Tablespace string
	Database   string
	Relation   string
	Block      string
}

func (a *Agent) initIOCache(cfg config.Config) error {
	const (
		driveOpsPerSec    = 150
		numVDevs          = 6
		drivesPerVDev     = 2
		headsPerDrive     = 4
		efficiency        = 0.5
		defaultIOCacheTTL = 86400 * time.Second

		maxConcurrentIOs = uint((driveOpsPerSec * numVDevs * drivesPerVDev * headsPerDrive) * efficiency)

		// ioCacheSize is set to cache all operations for ~100 WAL files
		ioCacheSize uint = 100 * uint(lsn.WALFileSize/lsn.WALPageSize)
	)

	ioCacheTTL := defaultIOCacheTTL

	// TODO(seanc@): Investigation is required to figure out if maxConcurrentIOs
	// should be clamped to the max number o pages in a given WAL file *
	// walReadAhead.  At some point the scheduling of the IOs is going to be more
	// burdensome than actually doing the IOs, but maybe that will only be a
	// visible problem with SSDs. ¯\_(ツ)_/¯
	a.maxConcurrentIOs = maxConcurrentIOs

	ioReqs := make(chan _IOCacheKey, a.maxConcurrentIOs)
	for ioWorker := 0; ioWorker < int(a.maxConcurrentIOs); ioWorker++ {
		a.ioCacheWG.Add(1)
		go func(threadID int) {
			log.Debug().Int("io-worker-thread-id", threadID).Msg("starting IO worker thread")
			defer func() {
				log.Debug().Int("io-worker-thread-id", threadID).Msg("shutting down IO worker thread")
				a.ioCacheWG.Done()
			}()

			for ioReq := range ioReqs {
				start := time.Now()

				if err := a.prefaultPage(ioReq); err != nil {
					log.Warn().Int("io-worker-thread-id", threadID).Err(err).
						Str("database", ioReq.Database).Str("relation", ioReq.Relation).
						Str("block", ioReq.Block).Msg("unable to prefault page")
				} else {
					a.metrics.Increment(metricsPrefaultCount)
				}

				a.metrics.RecordValue(metricsSysPreadLatency, float64(time.Now().Sub(start)/time.Millisecond))

				if a.isShuttingDown() {
					return
				}
			}
		}(ioWorker)
	}
	log.Info().Uint("io-worker-threads", a.maxConcurrentIOs).Msg("started IO worker threads")

	a.ioCache = gcache.New(int(ioCacheSize)).
		ARC().
		LoaderExpireFunc(func(key interface{}) (interface{}, *time.Duration, error) {
			select {
			case <-a.shutdownCtx.Done():
			case ioReqs <- key.(_IOCacheKey):
			}

			return struct{}{}, &ioCacheTTL, nil

		}).
		Build()

	return nil
}

// PageNum returns the pagenum of a given relation
func (ioCacheKey *_IOCacheKey) PageNum() (int64, error) {
	blockNumber, err := strconv.ParseUint(ioCacheKey.Block, 10, 64)
	if err != nil {
		log.Warn().Err(err).Str("str int", ioCacheKey.Block).Msgf("invalid integer: %+v", ioCacheKey)
		return -1, errors.Wrapf(err, "unable to parse block number")
	}

	pageNum := int64(blockNumber) % int64(lsn.MaxSegmentSize/lsn.WALPageSize)

	return pageNum, nil
}

func (a *Agent) prefaultPage(ioReq _IOCacheKey) error {
	pageNum, err := ioReq.PageNum()
	if err != nil {
		return errors.Wrapf(err, "unable to find the page number: %+v", ioReq)
	}

	fhCacheValue, err := a.fhCacheGetLocked(ioReq)
	if err != nil {
		return errors.Wrap(err, "unable to obtain file handle")
	}
	defer fhCacheValue.lock.RUnlock()

	var buf [lsn.WALPageSize]byte
	n, err := fhCacheValue.f.ReadAt(buf[:], pageNum*int64(lsn.WALPageSize))
	a.metrics.Add(metricsSysPreadBytes, uint64(n))
	if err != nil {
		if err != io.EOF {
			a.metrics.Increment(metricsXLogDumpErrorCount)
			return errors.Wrap(err, "unable to pread(2)")
		}

		return nil
	}

	return nil
}
