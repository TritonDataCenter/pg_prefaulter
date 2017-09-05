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
	"time"

	"github.com/bluele/gcache"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/joyent/pg_prefaulter/lsn"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
)

func (a *Agent) initIOReqCache(cfg config.Config) error {
	const (
		driveOpsPerSec        = 150
		numVDevs              = 6
		drivesPerVDev         = 2
		headsPerDrive         = 4
		efficiency            = 0.5
		maxConcurrentIOs      = uint((driveOpsPerSec * numVDevs * drivesPerVDev * headsPerDrive) * efficiency)
		ioReqCacheSize   uint = uint(lsn.WALFileSize / lsn.WALPageSize)
		ioReqCacheTTL         = 86400 * time.Second
	)

	a.maxConcurrentIOs = maxConcurrentIOs

	a.ioReqCache = gcache.New(int(ioReqCacheSize)).
		ARC().
		LoaderExpireFunc(func(key interface{}) (interface{}, *time.Duration, error) {
			rfKey, ok := key.(_RelationFileKey)
			if !ok {
				log.Panic().Msgf("unable to type assert key in IO Cache: %T %+v", rfKey, rfKey)
			}

			fdValueRaw, err := a.fdCache.Get(_FDCacheKey{_RelationFileKey: rfKey})
			if err != nil {
				log.Warn().Err(err).Msgf("unable to open file cache: %+v", rfKey)
				return struct{}{}, nil, err
			}

			fdValue, ok := fdValueRaw.(*_FDCacheValue)
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
						log.Warn().Err(err).Msgf("unable to re-open file: %+v", rfKey)
						return struct{}{}, nil, errors.Wrapf(err, "unable to re-open file: %+v", rfKey)
					}
				}
				// Hold onto our exclusive lock until we return.  We could in theory
				// loop and retry this operation with an RLock held but I'm okay with
				// making a few readers block in order to simplify the code.
				defer fdValue.lock.Unlock()
			}

			var buf [lsn.WALPageSize]byte
			pageNum, err := rfKey.PageNum()
			if err != nil {
				log.Warn().Err(err).Msgf("unable to find the page number: %+v", rfKey)
				return struct{}{}, nil, errors.Wrapf(err, "unable to find the page number: %+v", rfKey)
			}

			// FIXME(seanc@): Need to wrap this ReadAt() call in a wait group in
			// order to prevent IO starvation.
			start := time.Now()
			n, err := fdValue.fd.ReadAt(buf[:], pageNum*int64(lsn.WALPageSize))
			end := time.Now()

			a.metrics.RecordValue(metricsSysPreadLatency, float64(end.Sub(start)/time.Millisecond))
			a.metrics.Increment(metricsSysPreadCount)
			a.metrics.Add(metricsSysPreadCount, uint64(n))
			if err != nil {
				a.metrics.Increment(metricsXLogDumpErrorCount)
				log.Warn().Err(err).Str("database", rfKey.Database).Str("relation", rfKey.Relation).
					Str("block", rfKey.Block).Int64("page", pageNum).Int("n", n).Msg("error reading")
				return struct{}{}, nil, err
			}

			expire := ioReqCacheTTL

			return struct{}{}, &expire, nil
		}).
		Build()

	return nil
}
