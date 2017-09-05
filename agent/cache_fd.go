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
	"os"
	"sync"
	"time"

	"github.com/bluele/gcache"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

// _FDCacheKey is a comparable forward lookup key.
type _FDCacheKey struct {
	_RelationFileKey
}

// _FDCacheValue is a wrapper value type that includes an RWMutex.
type _FDCacheValue struct {
	_RelationFileKey

	// lock guards the remaining values.  The above _RelationFileKey values are
	// immutable.
	lock   *sync.RWMutex
	fd     *os.File // TODO(seanc@): rename to f
	isOpen bool
}

func (a *Agent) initFDCache(cfg config.Config) error {
	var numReservedFDs uint32 = 10
	var procNumFiles unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &procNumFiles); err != nil {
		return errors.Wrap(err, "unable to determine rlimits for number of files")
	}
	maxNumOpenFiles := uint32(procNumFiles.Cur) - a.walReadAhead

	// fdCache is private to ioReqCache
	fdCacheSize := maxNumOpenFiles - numReservedFDs
	fdCacheTTL := 3600 * time.Second

	a.fdCache = gcache.New(int(fdCacheSize)).
		ARC().
		LoaderExpireFunc(func(key interface{}) (interface{}, *time.Duration, error) {
			fdCacheKey, ok := key.(_FDCacheKey)
			if !ok {
				log.Panic().Msgf("unable to type assert key in file handle cache: %T %+v", key, key)
			}

			rf := _NewRelationFile(fdCacheKey)
			start := time.Now()
			fd, err := rf.Open()
			end := time.Now()
			if err != nil {
				log.Warn().Err(err).Msgf("unable to open relation file: %v", rf)
				return nil, nil, err
			}
			a.metrics.RecordValue(metricsSysOpenLatency, float64(end.Sub(start)/time.Millisecond))
			a.metrics.Increment(metricsSysOpenCount)

			// Return a valid value, unlocked.  gcache provides us with lock
			// coverage until we return.  Copies of this struct in different threads
			// will have an RLock() on the file handle.  Eviction will acquire a
			// Lock() and block on readers.  Consumers of this value will need to
			// either abort their operation when RLock() is acquired and isOpen is
			// false, or it will have to reacquire Lock and re-Open() File.
			val := _FDCacheValue{
				_RelationFileKey: rf._RelationFileKey,

				lock:   &sync.RWMutex{},
				fd:     fd,
				isOpen: true,
			}

			return &val, &fdCacheTTL, nil
		}).
		EvictedFunc(func(key, value interface{}) {
			val, ok := value.(*_FDCacheValue)
			if !ok {
				log.Panic().Msgf("bad, evicting something not a file handle: %+v", val)
			}

			val.lock.Lock()
			defer val.lock.Unlock()

			val.fd.Close()
			val.isOpen = false
			a.metrics.Increment(metricsSysCloseCount)
		}).
		Build()

	return nil
}
