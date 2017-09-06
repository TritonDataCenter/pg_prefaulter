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
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path"
	"regexp"
	"time"

	"github.com/bluele/gcache"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// Input to parse: rel 1663/16394/1249 blk 29
//                     ^^^^ ------------------- Tablespace ID
//                          ^^^^^ ------------- Database ID
//                                ^^^^ -------- Relation ID
//                                         ^^ - Block Number
var pgXLogDumpRE = regexp.MustCompile(`rel ([\d]+)/([\d]+)/([\d]+) blk ([\d]+)`)

// https://github.com/snaga/xlogdump
//
// [cur:CC/DFFF7C8, xid:448891062, rmid:11(Btree), len/tot_len:66/98, info:0, prev:C3/4FFF758] insert_leaf: s/d/r:1663/16385/16442 tid 1317010/91
// [cur:C4/70, xid:450806558, rmid:10(Heap), len/tot_len:737/769, info:0, prev:C4/20] insert: s/d/r:1663/16385/16431 blk/off:32400985/3 header: t_infomask2 12 t_infomask 2051 t_hoff 32
var xlogdumpRE = regexp.MustCompile(`s/d/r:([\d]+)/([\d]+)/([\d]+) (?:tid |blk/off:)([\d]+)`)

var (
	xlogRE *regexp.Regexp
)

func (a *Agent) initWALCache(cfg config.Config) error {
	// Create a worker pool of two WAL threads
	const numWALWorkers = 2
	walFiles := make(chan string)
	for walWorker := 0; walWorker < numWALWorkers; walWorker++ {
		a.walCacheWG.Add(1)
		go func(threadID int) {
			log.Debug().Int("wal-worker-thread-id", threadID).Msg("starting WAL worker thread")
			defer func() {
				log.Debug().Int("wal-worker-thread-id", threadID).Msg("shutting down WAL worker thread")
				a.walCacheWG.Done()
			}()

			for walFile := range walFiles {
				start := time.Now()

				if err := a.prefaultWALFile(walFile); err != nil {
					log.Error().Int("wal-worker-thread-id", threadID).Err(err).Str("wal filename", walFile).Msg("unable to prefault WAL file")
				} else {
					a.metrics.Increment(metricsWALFaultCount)
				}

				a.metrics.RecordValue(metricsWALFaultTime, float64(time.Now().Sub(start)/time.Second))
				if a.isShuttingDown() {
					return
				}
			}
		}(walWorker)
	}

	// Deliberately use a scan-intolerant cache because the inputs are going to be
	// ordered.  When the cache is queried, return a faux result and actually
	// perform the real work in a background goroutine.
	a.walCache = gcache.New(2 * int(a.walReadAhead)).
		LRU().
		LoaderFunc(func(key interface{}) (interface{}, error) {
			select {
			case <-a.shutdownCtx.Done():
			case walFiles <- key.(string):
			}

			return struct{}{}, nil
		}).
		Build()

	return nil
}

// prefaultWALFile shells out to pg_xlogdump(1) and reads its input.  The input
// from pg_xlogdump(1) is then turned into IO requests that are picked up and
// handled by the ioCache.
func (a *Agent) prefaultWALFile(walFile string) error {
	re := xlogRE.Copy()
	pgdataPath := viper.GetString(config.KeyPGData)
	var linesMatched, linesScanned, walFilesProcessed uint64

	fileName := path.Join(pgdataPath, "pg_xlog", walFile)

	_, err := os.Stat(fileName)
	if err != nil {
		log.Debug().Err(err).Msg("stat")
		return errors.Wrap(err, "WAL file does not exist")
	}

	cmd := exec.CommandContext(a.shutdownCtx, viper.GetString(config.KeyXLogPath), fileName)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		log.Debug().Err(err).Msg("exec")
		return errors.Wrapf(err, "unable to run %q", viper.GetString(config.KeyXLogPath))
	}

	if len(stdoutStderr) == 0 {
		log.Warn().Str("out", string(stdoutStderr)).Msg("unable to process WAL file")
		log.Debug().Msg("nada")
		return nil
	}

	scanner := bufio.NewScanner(bytes.NewReader(stdoutStderr))
	for scanner.Scan() {
		line := scanner.Bytes()
		linesScanned++
		submatches := re.FindAllSubmatch(line, -1)
		if submatches != nil {
			linesMatched++
		}
		for _, matches := range submatches {
			// TODO(seanc@): Send the IO requests here to a pool of io cache request
			// workers instead of fetching through the cache.  I think.
			_, err := a.ioCache.GetIFPresent(_IOCacheKey{
				Tablespace: string(matches[1]),
				Database:   string(matches[2]),
				Relation:   string(matches[3]),
				Block:      string(matches[4]),
			})
			if err == gcache.KeyNotFoundError {
				// cache miss
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Warn().Err(err).Msg("scanning output")
	}
	walFilesProcessed++

	a.metrics.Add(metricsXLogPrefaulted, walFilesProcessed)
	a.metrics.RecordValue(metricsXLogDumpLen, float64(len(stdoutStderr)))
	a.metrics.RecordValue(metricsXLogDumpLinesMatched, float64(linesMatched))
	a.metrics.RecordValue(metricsXLogDumpLinesScanned, float64(linesScanned))

	return nil
}
