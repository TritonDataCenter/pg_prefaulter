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
	"sync"
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
		driveOpsPerSec        = 150
		numVDevs              = 6
		drivesPerVDev         = 2
		headsPerDrive         = 4
		efficiency            = 0.5
		maxConcurrentIOs      = uint((driveOpsPerSec * numVDevs * drivesPerVDev * headsPerDrive) * efficiency)
		ioCacheSize      uint = uint(lsn.WALFileSize / lsn.WALPageSize)
		ioCacheTTL            = 86400 * time.Second
	)

	a.maxConcurrentIOs = maxConcurrentIOs

	a.ioCache = gcache.New(int(ioCacheSize)).
		ARC().
		LoaderExpireFunc(func(key interface{}) (interface{}, *time.Duration, error) {
			ioCacheKey, ok := key.(_IOCacheKey)
			if !ok {
				log.Panic().Msgf("unable to type assert key in IO Cache: %T %+v", ioCacheKey, ioCacheKey)
			}

			// FIXME(seanc@): the fdCache.Get(_NewFDCacheKey(...)) pattern here should
			// be replaced with a strict interface on the fdCache and fdCache
			// shouldn't have its interface exposed to callers like this.  Instead,
			// a.fdCacheGet() should be a helper method that does the right thing with
			// all of the type assertions, etc.
			fdValueRaw, err := a.fileHandleCache.Get(_NewFileHandleCacheKey(ioCacheKey))
			if err != nil {
				log.Warn().Err(err).Msgf("unable to open file cache: %+v", ioCacheKey)
				return struct{}{}, nil, err
			}

			fdValue, ok := fdValueRaw.(*_FileHandleCacheValue)
			if !ok {
				log.Panic().Msgf("unable to type assert file handle in IO Cache: %+v", fdValueRaw)
			}

			fdValue.lock.RLock()
			if fdValue.isOpen == true {
				defer fdValue.lock.RUnlock()
			} else {
				fdValue.lock.RUnlock()
				fdValue.lock.Lock()
				// Revalidate lock predicate with exclusive lock held
				if fdValue.isOpen == false {
					if _, err := fdValue.Open(); err != nil {
						fdValue.lock.Unlock()
						log.Warn().Err(err).Msgf("unable to re-open file: %+v", ioCacheKey)
						return struct{}{}, nil, errors.Wrapf(err, "unable to re-open file: %+v", ioCacheKey)
					}
				}
				// Hold onto our exclusive lock until we return.  We could in theory
				// loop and retry this operation with an RLock held but I'm okay with
				// making a few readers block in order to simplify the code.
				defer fdValue.lock.Unlock()
			}

			var buf [lsn.WALPageSize]byte
			pageNum, err := ioCacheKey.PageNum()
			if err != nil {
				log.Warn().Err(err).Msgf("unable to find the page number: %+v", ioCacheKey)
				return struct{}{}, nil, errors.Wrapf(err, "unable to find the page number: %+v", ioCacheKey)
			}

			// FIXME(seanc@): Need to wrap this ReadAt() call in a wait group in
			// order to prevent IO starvation.
			start := time.Now()
			n, err := fdValue.f.ReadAt(buf[:], pageNum*int64(lsn.WALPageSize))
			end := time.Now()

			a.metrics.RecordValue(metricsSysPreadLatency, float64(end.Sub(start)/time.Millisecond))
			a.metrics.Increment(metricsSysPreadCount)
			a.metrics.Add(metricsSysPreadCount, uint64(n))
			if err != nil {
				a.metrics.Increment(metricsXLogDumpErrorCount)
				log.Warn().Err(err).Str("database", ioCacheKey.Database).Str("relation", ioCacheKey.Relation).
					Str("block", ioCacheKey.Block).Int64("page", pageNum).Int("n", n).Msg("error reading")
				return struct{}{}, nil, err
			}

			expire := ioCacheTTL

			return struct{}{}, &expire, nil
		}).
		Build()

	return nil
}
