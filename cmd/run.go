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

package cmd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/alecthomas/units"
	"github.com/bluele/gcache"
	"github.com/joyent/pg_prefaulter/agent"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// walBlockSize == PostgreSQL's Page Size.  Page Size == BLKSZ
	//
	// TODO(seanc@): pull this data from `SHOW wal_block_size`.
	walBlockSize = 8 * units.KiB

	// walSegmentSize == PostgreSQL WAL File Size.
	//
	// TODO(seanc@): pull this value from `SHOW wal_segment_size`
	walSegmentSize = 16 * units.MiB

	// maxSegmentSize is the max size of a single file in a relation.
	//
	// TODO(seanc@): pull this value from pg_controldata(1)'s "Blocks per segment
	// of large relation" and multiply it by walBlockSize
	maxSegmentSize = 1 * units.GiB
)

// CLI arg values
var (
	fdCacheSize     uint
	fdCacheTTL           = 60 * time.Second
	ioReqCacheSize  uint = uint(walSegmentSize / walBlockSize)
	ioReqCacheTTL        = 60 * time.Second
	maxNumOpenFiles uint
	numReservedFDs  uint = 10
	numIOThreads    uint = 8
	pgXLogdumpMode  string
	pgXLogdumpPath  string
	walFiles        []string
	walReadAhead    uint
	walThreads      uint

	// derived variables from the CLI args
	xlogRE *regexp.Regexp
)

// CLI arg constants
const (
	pgXLogdumpModeLong    = "xlog-mode"
	pgXLogdumpModeShort   = "m"
	pgXLogdumpModeDefault = "pg"

	pgXLogdumpPathLong    = "xlogdump-bin"
	pgXLogdumpPathShort   = "x"
	pgXLogdumpPathDefault = "/usr/local/pg_xlogdump"

	walFilesLong  = "wal"
	walFilesShort = "w"

	walReadAheadLong    = "wal-readahead"
	walReadAheadShort   = "n"
	walReadAheadDefault = 4

	walThreadsLong    = "wal-threads"
	walThreadsShort   = "t"
	walThreadsDefault = 4
)

// Process-wide cache globals
var (
	cacheOnce  sync.Once
	ioReqCache gcache.Cache
)

// Process-wide stats, all managed as atomic integers
var (
	walReadOps    uint64 // Number of pread(2) operations
	walBytesRead  uint64 // Number of bytes pread(2)
	walReadErrors uint64 // Number of pread(2) errors
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run pg_prefaulter",
	Long:  `Run pg_prefaulter and begin faulting in PostgreSQL pages`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		log.Debug().Msgf("args: %v", args)
		defer func() {
			// FIXME(seanc@): Iterate over known viper keys and automatically log
			// values.
			log.Debug().
				Str(config.KeyPGData, viper.GetString(config.KeyPGData)).
				Str(config.KeyPGHost, viper.GetString(config.KeyPGHost)).
				Str(config.KeyPGUser, viper.GetString(config.KeyPGUser)).
				Str(config.KeyPGPassword, viper.GetString(config.KeyPGPassword)). // FIXME(seanc@): // Reset to <redacted>
				Str(pgXLogdumpModeLong, pgXLogdumpMode).
				Str(pgXLogdumpPathLong, pgXLogdumpPath).
				Dur(config.KeyPollInterval, viper.GetDuration(config.KeyPollInterval)).
				Str(walFilesLong, strings.Join(walFiles, ", ")).
				Uint(walReadAheadLong, walReadAhead).
				Uint(walThreadsLong, walThreads).
				Uint("io-req-dedup-size", ioReqCacheSize).
				Int("num wal files", len(walFiles)).
				Msg("flags")
		}()

		if len(walFiles) == 0 {
			return fmt.Errorf("no WAL files specified")
		}

		var procNumFiles unix.Rlimit
		if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &procNumFiles); err != nil {
			return errors.Wrap(err, "unable to determine rlimits for number of files")
		}
		maxNumOpenFiles = uint(procNumFiles.Cur) - walReadAhead

		switch pgXLogdumpMode {
		case "xlog":
			xlogRE = xlogdumpRE.Copy()
		case "pg":
			xlogRE = pgXLogDumpRE.Copy()
		default:
			return fmt.Errorf("unsupported %s: %q", pgXLogdumpModeLong, pgXLogdumpMode)
		}

		// Scale the ioReqCacheSize to match the number of WAL files we're going to
		// snarf
		ioReqCacheSize = walReadAhead * uint((walSegmentSize / walBlockSize))

		initCaches()

		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		log.Debug().Msg("Starting WAL reader")

		// Unconditionally emit various stats on shutdown
		defer func() {
			log.Info().
				Uint("num-wal-threads", numIOThreads).
				Uint64("wal-read-ops", atomic.LoadUint64(&walReadOps)).
				Uint64("wal-bytes-read", atomic.LoadUint64(&walBytesRead)).
				Uint64("wal-read-errors", atomic.LoadUint64(&walReadErrors)).
				Msg("boss thread stats")
		}()

		a, err := agent.New(config.NewDefault())
		if err != nil {
			return errors.Wrap(err, "unable to start the pg_prefaulter agent")
		}
		go a.Start()
		defer a.Stop()

		return a.Wait()

		// var wg sync.WaitGroup

		// // clamp the number of threads to the number of walFiles
		// if numFiles := uint(len(walFiles)); walThreads > numFiles {
		// 	walThreads = numFiles
		// }

		// // Start WAL worker threads
		// ioRequestCh := make(chan *_RelationFile)
		// // Create a gang of IO workers for this boss
		// for i := uint(0); i < numIOThreads; i++ {
		// 	walWorkerThreadID := i // pin i's value
		// 	go walFaultWorker(walWorkerThreadID, ioRequestCh)
		// }

		// // Add an item to the wait group
		// walFilesCh := make(chan string, walThreads)
		// for i := uint(0); i < walThreads; i++ {
		// 	i := i
		// 	go func() {
		// 		wg.Add(1)
		// 		defer wg.Done()
		// 		walBossThread(shutdownCtxt, i, ioRequestCh, walFilesCh)
		// 	}()
		// }

		// // single thread feeder to one boss-thread per WAL file
		// for _, walFile := range walFiles {
		// 	log.Debug().Str("filename", walFile).Msg("submitting file")
		// 	walFilesCh <- walFile
		// }
		// log.Debug().Msg("closing walfiles channel")
		// close(walFilesCh)

		// wg.Wait()

		// return nil
	},
}

func init() {
	RootCmd.AddCommand(runCmd)

	const (
		defaultPollInterval = "1s"
		pollIntervalLong    = "poll-interval"
	)

	runCmd.Flags().StringP(pollIntervalLong, "i", defaultPollInterval, "Interval at which pg_prefaulter polls the database for state change")
	viper.BindPFlag(config.KeyPollInterval, runCmd.Flags().Lookup(pollIntervalLong))

	runCmd.Flags().StringVarP(&pgXLogdumpPath, pgXLogdumpPathLong, pgXLogdumpPathShort,
		pgXLogdumpPathDefault, "Path to pg_xlogdump(1)")
	runCmd.Flags().StringVarP(&pgXLogdumpMode, pgXLogdumpModeLong, pgXLogdumpModeShort,
		pgXLogdumpModeDefault, `pg_xlogdump(1) variant: "xlog" or "pg"`)
	runCmd.Flags().StringArrayVarP(&walFiles, walFilesLong, walFilesShort,
		walFiles, "WAL files to investigate")
	runCmd.Flags().UintVarP(&walReadAhead, walReadAheadLong, walReadAheadShort,
		walReadAheadDefault, "Number of WAL entries to perform read-ahead into")
	runCmd.Flags().UintVarP(&walThreads, walThreadsLong, walThreadsShort,
		walThreadsDefault, "Number of conurrent prefetch threads per WAL file")
}

// _FDCacheValue is a wrapper value type that includes an RWMutex.
type _FDCacheValue struct {
	*sync.RWMutex
	_RelationFile
	os.File
	isOpen bool
}

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

// walBossThread is a boss thread for a pool of worker theads.  It is the job of
// the boss thread to popen(3) the xlog dumping utility and spawn worker threads
// that will pre-fault in de-duped page requests into the OS'es filesystem cache
// (global variable ioReqCache).
func walBossThread(ctx context.Context, threadID uint,
	ioRequestCh chan *_RelationFile, in <-chan string) {
	log.Debug().Uint("boss-thread-id", threadID).Msg("starting thread")
	var linesScanned, matchedLines, dispatchCount, walFilesProcessed uint64

	re := xlogRE.Copy()
	pgdataPath := viper.GetString(config.KeyPGData)

	// Loop until we've processed all WAL files
	for {
		select {
		case <-ctx.Done():
			return
		case walFile, ok := <-in:
			if !ok {
				close(ioRequestCh)
				return
			}
			fileName := path.Join(pgdataPath, "pg_xlog", walFile)
			log.Debug().Str("filename", fileName).Msg("reading walfile")

			_, err := os.Stat(fileName)
			if err != nil {
				log.Warn().Err(err).Msg("WAL file does not exist")
				continue
			}

			cmd := exec.CommandContext(ctx, pgXLogdumpPath, fileName)
			stdoutStderr, err := cmd.CombinedOutput()
			if err != nil || len(stdoutStderr) == 0 {
				log.Warn().Str("out", string(stdoutStderr)).Err(err).Msg("unable to process WAL file")
				continue
			}
			log.Debug().Int("wal output len", len(stdoutStderr)).Msg("post-xlogdump stats")

			scanner := bufio.NewScanner(bytes.NewReader(stdoutStderr))
			for scanner.Scan() {
				line := scanner.Bytes()
				linesScanned++
				submatches := re.FindAllSubmatch(line, -1)
				if submatches != nil {
					matchedLines++
				}
				for _, matches := range submatches {
					ioRequestCh <- &_RelationFile{
						Tablespace: string(matches[1]),
						Database:   string(matches[2]),
						Relation:   string(matches[3]),
						Block:      string(matches[4]),
					}
					dispatchCount++
				}
			}
			if err := scanner.Err(); err != nil {
				log.Warn().Err(err).Msg("scanning output")
			}
			walFilesProcessed++

			// Small summary of work done while processing a single WAL file
			log.Debug().Str("walfile", walFile).
				Uint64("dispatch count", dispatchCount).
				Uint64("lines scanned", linesScanned).
				Uint64("matched lines", matchedLines).
				Msg("wal boss stats")
		}
	}
}

// walFaultWorker receives IO instructions from its boss and demand loads pages
// by querying through the ARC, which actually faults the page in, or returns a
// cache hit if the page has been accessed frequently.  This makes the
// assumption that the OS's FS cache hasn't evicted the page by the time it
// falls out of the ioReqCache cache.  If this is happening, reducing the TTL on
// the ioReqCache cache should provide a guard against artificially promoting
// the page from an ARC's MRU to its MFU (read: this guards against a PostgreSQL
// HOT UPDATE from artificially moving the page within the OS'es FS Cache from
// its MRU list to its MFU list).
func walFaultWorker(threadID uint, in <-chan *_RelationFile) {
	log.Debug().Uint("worker-thread-id", threadID).Msg("starting worker thread")

	defer func() {
		log.Debug().
			Uint("worker-thread-id", threadID).
			Str("action", "shutdown").
			Msg("worker thread")
	}()

	for {
		select {
		case ioReq, ok := <-in:
			if !ok {
				log.Debug().Uint("worker-thread-id", threadID).Msg("shutting down, input closed")
				return
			}

			// The payload is discarded because we're not scanning the input.
			_, err := ioReqCache.Get(ioReq)
			if err != nil {
				// NOTE(seanc@): it may be bogus for us to Warn() on error because the
				// relation could have changed out from under us while prefaulting in
				// pages.  For now it seems prudent to log an error knowing it may be
				// spurious.
				log.Warn().Err(err).Msgf("fetching file %+v", ioReq)
				continue
			}
		}
	}
}

// initCaches initializes the various caches.  initCache() is thread-safe but
// assumes it will only be called once at program initialization time.  The
// first call to initCache sizes the cache.  The ioCache handles the file
// descriptor caching and the faulting of the page using the key as input.  The
// two layers of caching used are:
//
// 1) An IO instruction cache that faults a given page given a relation file and
//    page.  The IO instruction cache pulls file descriptors from its FD Cache.
// 2) A file-level cache that returns open file handles.
//
// The two layers of caching are required in order to effectively implement a
// multi-index with the first index being used to lookup the file handle, the
// second index being used to cache operations performed against a given file.
func initCaches() {
	cacheOnce.Do(func() {
		// fdCache is private to ioReqCache
		fdCacheSize := maxNumOpenFiles - numReservedFDs

		fdCache := gcache.New(int(fdCacheSize)).
			ARC().
			LoaderExpireFunc(func(key interface{}) (interface{}, *time.Duration, error) {
				rf, ok := key.(*_RelationFile)
				if !ok {
					log.Panic().Msgf("unable to type assert key in file handle cache: %T %+v", key, key)
				}

				f, err := rf.Open()
				if err != nil {
					log.Warn().Err(err).Msgf("unable to open relation file: %v", rf)
					return nil, nil, err
				}

				// Return a valid value, unlocked.  gcache provides us with lock
				// coverage until we return.  Copies of this struct in different threads
				// will have an RLock() on the file handle.  Eviction will acquire a
				// Lock() and block on readers.  Consumers of this value will need to
				// either abort their operation when RLock() is acquired and isOpen is
				// false, or it will have to reacquire Lock and re-Open() File.
				val := &_FDCacheValue{
					_RelationFile: *rf,
					File:          *f,
					isOpen:        true,
				}

				expire := fdCacheTTL
				return val, &expire, nil
			}).
			EvictedFunc(func(key, value interface{}) {
				f, ok := value.(*_FDCacheValue)
				if !ok {
					log.Panic().Msgf("bad, evicting something not a file handle: %+v", f)
				}

				f.Lock()
				defer f.Unlock()

				f.Close()
				f.isOpen = false
			}).
			Build()

		ioReqCache = gcache.New(int(ioReqCacheSize)).
			ARC().
			LoaderExpireFunc(func(key interface{}) (interface{}, *time.Duration, error) {
				rf, ok := key.(*_RelationFile)
				if !ok {
					log.Panic().Msgf("unable to type assert key in IO Cache: %T %+v", key, key)
				}

				fRaw, err := fdCache.Get(rf)
				if err != nil {
					log.Warn().Err(err).Msgf("unable to open file cache: %+v", rf)
					return struct{}{}, nil, err
				}

				f, ok := fRaw.(*_FDCacheValue)
				if !ok {
					log.Panic().Msgf("unable to type assert file handle in IO Cache: %+v", fRaw)
				}

				f.RLock()
				if f.isOpen == true {
					defer f.RUnlock()
				} else {
					f.RUnlock()
					f.Lock()
					// Revalidate lock predicate with exclusive lock held
					if f.isOpen == false {
						if _, err := f.Open(); err != nil {
							f.Unlock()
							log.Warn().Err(err).Msgf("unable to re-open file: %+v", rf)
							return struct{}{}, nil, errors.Wrapf(err, "unable to re-open file: %+v", rf)
						}
					}
					// Hold onto our exclusive lock until we return.  We could in theory
					// loop and retry this operation with an RLock held but I'm okay with
					// making a few readers block in order to simplify the code.
					defer f.Unlock()
				}

				var buf [walBlockSize]byte
				pageNum, err := rf.PageNum()
				if err != nil {
					log.Warn().Err(err).Msgf("unable to find the page number: %+v", rf)
					return struct{}{}, nil, errors.Wrapf(err, "unable to find the page number: %+v", rf)
				}
				n, err := f.ReadAt(buf[:], pageNum*int64(walBlockSize))
				atomic.AddUint64(&walReadOps, 1)
				atomic.AddUint64(&walBytesRead, uint64(n))
				if err != nil {
					atomic.AddUint64(&walReadErrors, 1)
					log.Warn().Err(err).Str("database", rf.Database).Str("relation", rf.Relation).
						Str("block", rf.Block).Int64("page", pageNum).Int("n", n).Msg("error reading")
					return struct{}{}, nil, err
				}

				expire := ioReqCacheTTL

				// Store an empty value in the IoReqDedup cache
				return struct{}{}, &expire, nil
			}).
			Build()

		log.Debug().
			Uint("io-req-dedup size", ioReqCacheSize).
			Msg("caches initialized")
	})
}
