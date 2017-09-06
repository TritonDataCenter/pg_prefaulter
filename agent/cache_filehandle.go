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
	"os"
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
	"golang.org/x/sys/unix"
)

// _FileHandleCacheKey is a comparable forward lookup key.
type _FileHandleCacheKey struct {
	_IOCacheKey
}

// _FileHandleCacheValue is a wrapper value type that includes an RWMutex.
type _FileHandleCacheValue struct {
	_FileHandleCacheKey

	// lock guards the remaining values.  The values in the _FileHandleCacheKey
	// are immutable and therefore do not need to be guarded by a lock.  WTB
	// `const` modifier for compiler enforced immutability.  Where's my C++ when I
	// need it?
	lock   *sync.RWMutex
	f      *os.File
	isOpen bool
}

func _NewFileHandleCacheKey(ioCacheKey _IOCacheKey) _FileHandleCacheKey {
	return _FileHandleCacheKey{
		_IOCacheKey: ioCacheKey,
	}
}

// Open calculates the relation filename and returns an open file handle.
//
// TODO(seanc@): Change the logic of this method to use the
// lsn type.
func (fhCacheKey *_FileHandleCacheKey) Open() (*os.File, error) {
	blockNumber, err := strconv.ParseUint(fhCacheKey.Block, 10, 64)
	if err != nil {
		log.Warn().Err(err).Str("str int", fhCacheKey.Block).Msgf("invalid integer: %+v", fhCacheKey)
		return nil, errors.Wrapf(err, "unable to parse block number")
	}

	// FIXME(seanc@): Use the logic in the lsn package
	fileNum := int64(blockNumber) / int64(lsn.MaxSegmentSize/lsn.WALPageSize)
	filename := fhCacheKey.Relation
	if fileNum > 0 {
		// It's easier to abuse Relation here than to support a parallel refilno
		// struct member
		filename = fmt.Sprintf("%s.%d", fhCacheKey.Relation, fileNum)
	}

	filename = path.Join(viper.GetString(config.KeyPGData), "base", string(fhCacheKey.Database), string(filename))

	f, err := os.Open(filename)
	if err != nil {
		log.Warn().Err(err).Msgf("unable to open relation name %q", filename)
		return nil, errors.Wrapf(err, "unable to open relation name %q", filename)
	}

	return f, nil
}

func (a *Agent) initFileHandleCache(cfg config.Config) error {
	var numReservedFDs uint32 = 10
	var procNumFiles unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &procNumFiles); err != nil {
		return errors.Wrap(err, "unable to determine rlimits for number of files")
	}
	maxNumOpenFiles := uint32(procNumFiles.Cur) - a.walReadAhead

	fhCacheSize := maxNumOpenFiles - numReservedFDs
	fhCacheTTL := 3600 * time.Second

	a.fileHandleCache = gcache.New(int(fhCacheSize)).
		ARC().
		LoaderExpireFunc(func(fhCacheKeyRaw interface{}) (interface{}, *time.Duration, error) {
			fhCacheKey, ok := fhCacheKeyRaw.(_FileHandleCacheKey)
			if !ok {
				log.Panic().Msgf("unable to type assert key in file handle cache: %T %+v", fhCacheKeyRaw, fhCacheKeyRaw)
			}

			start := time.Now()
			f, err := fhCacheKey.Open()
			end := time.Now()
			if err != nil {
				log.Warn().Err(err).Msgf("unable to open relation file: %+v", fhCacheKey)
				return nil, nil, err
			}
			a.metrics.RecordValue(metricsSysOpenLatency, float64(end.Sub(start)/time.Microsecond))
			a.metrics.Increment(metricsSysOpenCount)

			// Return a valid value, unlocked.  gcache provides us with lock coverage
			// until we return.  Copies of this struct in different threads will have
			// an RLock() on the file handle.  Eviction will acquire a Lock() and
			// block on readers.  Consumers of this value will need to either abort
			// their operation when RLock() is acquired and isOpen is false, or it
			// will have to reacquire Lock and re-Open() File.
			fhCacheVal := _FileHandleCacheValue{
				_FileHandleCacheKey: fhCacheKey,

				lock:   &sync.RWMutex{},
				f:      f,
				isOpen: true,
			}

			return &fhCacheVal, &fhCacheTTL, nil
		}).
		EvictedFunc(func(fhCacheKeyRaw, fhCacheValueRaw interface{}) {
			fhCacheValue, ok := fhCacheValueRaw.(*_FileHandleCacheValue)
			if !ok {
				log.Panic().Msgf("bad, evicting something not a file handle: %+v", fhCacheValue)
			}

			fhCacheValue.lock.Lock()
			defer fhCacheValue.lock.Unlock()

			fhCacheValue.f.Close()
			fhCacheValue.isOpen = false
			a.metrics.Increment(metricsSysCloseCount)
		}).
		Build()

	return nil
}
