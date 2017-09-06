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
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/bluele/gcache"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/joyent/pg_prefaulter/lsn"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
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

// _IOCacheValue contains the forward lookup information for a given relation
// file.  _IOCacheValue is a
// [comparable](https://golang.org/ref/spec#Comparison_operators) struct used as
// a lookup key.  These values are immutable and map 1:1 with the string inputs
// read from the xlog scanning utility.
type _IOCacheValue struct {
	_IOCacheKey

	lock     sync.Mutex
	filename string
	pageNum  int64
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

	ioReqs := make(chan _IOCacheKey)
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

// Filename returns the filename of a given relation
func (ioCacheValue *_IOCacheValue) Filename() (string, error) {
	ioCacheValue.lock.Lock()
	defer ioCacheValue.lock.Unlock()
	if ioCacheValue.filename == "" {
		if err := ioCacheValue.populateSelf(); err != nil {
			log.Warn().Err(err).Msg("populating self")
			return "", errors.Wrap(err, "populating self")
		}
	}

	return ioCacheValue.filename, nil
}

// PageNum returns the pagenum of a given relation
func (ioCacheValue *_IOCacheValue) PageNum() (int64, error) {
	ioCacheValue.lock.Lock()
	defer ioCacheValue.lock.Unlock()
	if ioCacheValue.filename == "" {
		if err := ioCacheValue.populateSelf(); err != nil {
			log.Warn().Err(err).Msg("populating self")
			return -1, errors.Wrap(err, "populating self")
		}
	}

	return ioCacheValue.pageNum, nil
}

func (ioCacheValue *_IOCacheValue) populateSelf() error {
	blockNumber, err := strconv.ParseUint(ioCacheValue.Block, 10, 64)
	if err != nil {
		log.Warn().Err(err).Str("str int", ioCacheValue.Block).Msgf("invalid integer: %+v", ioCacheValue)
		return errors.Wrapf(err, "unable to parse block number")
	}

	ioCacheValue.pageNum = int64(blockNumber) % int64(lsn.MaxSegmentSize/lsn.WALPageSize)
	fileNum := int64(blockNumber) / int64(lsn.MaxSegmentSize/lsn.WALPageSize)
	filename := ioCacheValue.Relation
	if fileNum > 0 {
		// It's easier to abuse Relation here than to support a parallel refilno
		// struct member
		filename = fmt.Sprintf("%s.%d", ioCacheValue.Relation, fileNum)
	}

	ioCacheValue.filename = path.Join(viper.GetString(config.KeyPGData), "base", string(ioCacheValue.Database), string(filename))

	return nil
}

func (a *Agent) prefaultPage(ioReq _IOCacheKey) error {
	pageNum, err := ioReq.PageNum()
	if err != nil {
		return errors.Wrapf(err, "unable to find the page number: %+v", ioReq)
	}

	var buf [lsn.WALPageSize]byte
	fhCacheValue, exclusiveLock, err := a.fhCacheGetLocked(ioReq)
	switch {
	case err != nil:
		return errors.Wrap(err, "unable to obtain file handle")
	case exclusiveLock:
		defer fhCacheValue.lock.Unlock()
	default:
		defer fhCacheValue.lock.RUnlock()
	}

	n, err := fhCacheValue.f.ReadAt(buf[:], pageNum*int64(lsn.WALPageSize))
	a.metrics.Add(metricsSysPreadBytes, uint64(n))
	if err != nil {
		a.metrics.Increment(metricsXLogDumpErrorCount)
		return errors.Wrap(err, "unable to pread(2)")
	}

	return nil
}

func (a *Agent) fhCacheGetLocked(ioReq _IOCacheKey) (fhCacheValue *_FileHandleCacheValue, exclusiveLock bool, err error) {
	// FIXME(seanc@): the fdCache.Get(_NewFDCacheKey(...)) pattern here should be
	// replaced with a strict interface on the fdCache and fdCache shouldn't have
	// its interface exposed to callers like this.  Instead, a.fdCacheGet() should
	// be a helper method that does the right thing with all of the type
	// assertions, etc.  Or: a.fdCacheGetLocked()
	fhValueRaw, err := a.fileHandleCache.Get(_NewFileHandleCacheKey(ioReq))
	if err != nil {
		log.Warn().Err(err).Msgf("unable to open file cache: %+v", ioReq)
		return nil, false, err
	}

	fhValue, ok := fhValueRaw.(*_FileHandleCacheValue)
	if !ok {
		log.Panic().Msgf("unable to type assert file handle in IO Cache: %+v", fhValueRaw)
	}

	fhValue.lock.RLock()
	if fhValue.isOpen == true {
		return fhValue, false, nil
	}

	fhValue.lock.RUnlock()
	fhValue.lock.Lock()
	// Revalidate lock predicate with exclusive lock held
	if fhValue.isOpen == false {
		if _, err := fhValue.Open(); err != nil {
			fhValue.lock.Unlock()
			return nil, false, errors.Wrapf(err, "unable to re-open file: %+v", fhValue._FileHandleCacheKey)
		}
	}

	// force the caller to deal with an exclusive lock in order to not drop
	// fhValue.lock.  Busy-looping on this lock attempting to demote the lock
	// isn't worth the hassle.
	return fhValue, true, nil
}
