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

package walcache

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/alecthomas/units"
	"github.com/bluele/gcache"
	cgm "github.com/circonus-labs/circonus-gometrics"
	"github.com/joyent/pg_prefaulter/agent/iocache"
	"github.com/joyent/pg_prefaulter/agent/structs"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/joyent/pg_prefaulter/lib"
	"github.com/joyent/pg_prefaulter/pg"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
)

// Input to parse: rel 1663/16394/1249 blk 29
//                     ^^^^ ------------------- Tablespace ID
//                          ^^^^^ ------------- Database ID
//                                ^^^^ -------- Relation ID
//                                         ^^ - Block Number
var pgXLogDumpRE = regexp.MustCompile(`rel ([\d]+)/([\d]+)/([\d]+) (?:fork [^\s]+ )?blk ([\d]+)`)

// https://github.com/snaga/xlogdump
//
// [cur:CC/DFFF7C8, xid:448891062, rmid:11(Btree), len/tot_len:66/98, info:0, prev:C3/4FFF758] insert_leaf: s/d/r:1663/16385/16442 tid 1317010/91
// [cur:C4/70, xid:450806558, rmid:10(Heap), len/tot_len:737/769, info:0, prev:C4/20] insert: s/d/r:1663/16385/16431 blk/off:32400985/3 header: t_infomask2 12 t_infomask 2051 t_hoff 32
var xlogdumpRE = regexp.MustCompile(`s/d/r:([\d]+)/([\d]+)/([\d]+) (?:tid |blk/off:)([\d]+)`)

// WALCache is a read-through cache to:
//
// a) provide a reentrant interface
// b) deduplicate requests (i.e. no thundering-herd for the same WAL file)
// c) deliberately intollerant of scans because we know the input is monotonic
// d) sized to include only the KeyWALReadahead
type WALCache struct {
	ctx context.Context
	wg  sync.WaitGroup
	cfg *config.WALCacheConfig

	purgeLock sync.Mutex
	c         gcache.Cache
	ioCache   *iocache.IOCache

	re      *regexp.Regexp
	metrics *cgm.CirconusMetrics
}

func New(ctx context.Context, cfg *config.Config, metrics *cgm.CirconusMetrics, ioCache *iocache.IOCache) (*WALCache, error) {
	wc := &WALCache{
		ctx:     ctx,
		metrics: metrics,
		cfg:     &cfg.WALCacheConfig,

		ioCache: ioCache,
	}

	switch cfg.WALCacheConfig.Mode {
	case config.WALModeXLog:
		wc.re = xlogdumpRE.Copy()
	case config.WALModePG:
		wc.re = pgXLogDumpRE.Copy()
	default:
		panic(fmt.Sprintf("unsupported WALConfig.mode: %v", cfg.WALCacheConfig.Mode))
	}

	walWorkers := int(math.Ceil(float64(wc.cfg.ReadaheadBytes) / float64(pg.WALFileSize)))
	walFilePrefaultWorkQueue := make(chan pg.WALFilename)
	for walWorker := 0; walWorker < walWorkers; walWorker++ {
		wc.wg.Add(1)
		go func(threadID int) {
			defer func() {
				wc.wg.Done()
			}()

			const heartbeat = 60 * time.Second
			for {
				select {
				case <-wc.ctx.Done():
					return
				case <-time.After(heartbeat):
				case walFile, ok := <-walFilePrefaultWorkQueue:
					if !ok {
						return
					}

					start := time.Now()

					if err := wc.prefaultWALFile(walFile); err != nil {
						log.Error().Int("wal-worker-thread-id", threadID).Err(err).
							Str("wal filename", string(walFile)).
							Msg("unable to prefault WAL file")
					} else {
						wc.metrics.Increment(config.MetricsWALFaultCount)
					}

					wc.metrics.RecordValue(config.MetricsWALFaultTime, float64(time.Now().Sub(start)/time.Second))
				}
			}
		}(walWorker)
	}
	log.Info().Int("wal-worker-threads", walWorkers).Msg("started WAL worker threads")

	// Deliberately use a scan-intolerant cache because the inputs are going to be
	// ordered.  When the cache is queried, return a faux result and actually
	// perform the real work in a background goroutine.
	wc.c = gcache.New(2 * int(walWorkers)).
		LRU().
		LoaderFunc(func(keyRaw interface{}) (interface{}, error) {
			walFilename := keyRaw.(pg.WALFilename)

			select {
			case <-wc.ctx.Done():
			case walFilePrefaultWorkQueue <- walFilename:
			}

			return true, nil
		}).
		Build()

	go lib.LogCacheStats(wc.ctx, wc.c, "walcache-stats")

	return wc, nil
}

// Get forwards to gcache.Cache's Get().
func (wc *WALCache) Get(k interface{}) (interface{}, error) {
	return wc.c.Get(k)
}

// GetIFPresent forwards to gcache.Cache's GetIFPresent().
func (wc *WALCache) GetIFPresent(k interface{}) (interface{}, error) {
	return wc.c.GetIFPresent(k)
}

// Purge purges the WALCache of its cache (and all downstream caches)
func (wc *WALCache) Purge() {
	wc.purgeLock.Lock()
	defer wc.purgeLock.Unlock()

	wc.c.Purge()
	wc.ioCache.Purge()
}

// Readahead returns the number of WAL files to read ahead of PostgreSQL.
func (wc *WALCache) Readahead() units.Base2Bytes {
	return wc.cfg.ReadaheadBytes
}

// Wait blocks until the WALCache finishes shutting down its workers (including
// the workers of its IOCache).
func (wc *WALCache) Wait() {
	wc.wg.Wait()
	wc.ioCache.Wait()
}

// prefaultWALFile shells out to pg_xlogdump(1) and reads its input.  The input
// from pg_xlogdump(1) is then turned into IO requests that are picked up and
// handled by the ioCache.
func (wc *WALCache) prefaultWALFile(walFile pg.WALFilename) (err error) {
	re := wc.re.Copy()
	var linesMatched, linesScanned, walFilesProcessed uint64

	walFileAbs := path.Join(wc.cfg.PGDataPath, "pg_xlog", string(walFile))
	_, err = os.Stat(walFileAbs)
	if err != nil {
		log.Debug().Err(err).Msg("stat")
		return errors.Wrap(err, "WAL file does not exist")
	}

	var pgDumpXLOGOut []byte
	cmd := exec.CommandContext(wc.ctx, wc.cfg.XLogDumpPath, walFileAbs)
	cmd.Stderr = ioutil.Discard
	pgDumpXLOGOut, err = cmd.Output()

	// pg_xlogdump(1) can return 1 when it has problems decoding output.  Notably
	// this can occur with corrupt or records that can't be parsed fully.  For
	// instance:
	//
	// pg_xlogdump: FATAL:  error in WAL record at C/A15FD930: record with incorrect prev-link 61313664/37303561 at C/A15FD968
	//
	// As such, only bail if we have an error and there wasn't any stdout output.
	if len(pgDumpXLOGOut) == 0 && err != nil {
		log.Debug().Err(err).Str("pg_xlogdump-path", wc.cfg.XLogDumpPath).Str("walfile", walFileAbs).Msg("pg_xlogdump execve(2) failed")
		return errors.Wrapf(err, "unable to run %q", wc.cfg.XLogDumpPath)
	}

	scanner := bufio.NewScanner(bytes.NewReader(pgDumpXLOGOut))
	for scanner.Scan() {
		line := scanner.Bytes()
		linesScanned++
		submatches := re.FindAllSubmatch(line, -1)
		if submatches != nil {
			linesMatched++
		}

		for _, matches := range submatches {
			tablespace, err := strconv.ParseUint(string(matches[1]), 10, 64)
			if err != nil {
				log.Error().Err(err).Str("input", string(matches[1])).Msg("unable to convert tablespace")
				continue
			}

			database, err := strconv.ParseUint(string(matches[2]), 10, 64)
			if err != nil {
				log.Error().Err(err).Str("input", string(matches[2])).Msg("unable to convert database")
				continue
			}

			relation, err := strconv.ParseUint(string(matches[3]), 10, 64)
			if err != nil {
				log.Error().Err(err).Str("input", string(matches[3])).Msg("unable to convert relation")
				continue
			}

			block, err := strconv.ParseUint(string(matches[4]), 10, 64)
			if err != nil {
				log.Error().Err(err).Str("input", string(matches[4])).Msg("unable to convert block")
				continue
			}

			// Send all IOs through the non-blocking cache interface.  Leave it up to
			// the ARC cache to deal with the influx of go routines which will get
			// scheduled and rate limited behind the ioCache.  If this ends up
			// becoming a problem we could throttle the requests into the cache, but I
			// really hope that's not something we need to do.
			//
			// Worst case is we flood the ioCache with requests and then block on the
			// next WALfile.  Because the max number of pages per WAL file is finite
			// (16MiB/8KiB == ~2K), at most we should have 2K threads running *
			// KeyWALReadahead.  That's very survivable for now but can be optimized
			// if necessary.
			ioCacheKey := structs.IOCacheKey{
				Tablespace: pg.OID(tablespace),
				Database:   pg.OID(database),
				Relation:   pg.OID(relation),
				Block:      pg.HeapBlockNumber(block),
			}
			_, err = wc.ioCache.GetIFPresent(ioCacheKey)
			switch {
			case err == nil:
			case err == gcache.KeyNotFoundError:
				// cache miss, an IO has been scheduled in the background.
			case err != nil:
				log.Debug().Err(err).Msg("iocache prefaultWALFile()")
			}
		}
	}
	if err = scanner.Err(); err != nil {
		log.Warn().Err(err).Msg("scanning output")
	}

	// For whatever reason pg_xlogdump(1) succeeded but produced no output
	if len(pgDumpXLOGOut) > 0 {
		walFilesProcessed++
	} else {
		log.Warn().Str("out", string(pgDumpXLOGOut)).Msgf("unable to process WAL file: %+q", pgDumpXLOGOut)
	}

	wc.metrics.Add(config.MetricsXLogPrefaulted, walFilesProcessed)
	wc.metrics.RecordValue(config.MetricsXLogDumpLen, float64(len(pgDumpXLOGOut)))
	wc.metrics.RecordValue(config.MetricsXLogDumpLinesMatched, float64(linesMatched))
	wc.metrics.RecordValue(config.MetricsXLogDumpLinesScanned, float64(linesScanned))

	return nil
}
