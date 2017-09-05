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
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/alecthomas/units"
	"github.com/bluele/gcache"
	cgm "github.com/circonus-labs/circonus-gometrics"
	"github.com/jackc/pgx"
	"github.com/joyent/pg_prefaulter/buildtime"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/joyent/pg_prefaulter/lsn"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

const (
	// stacktrace buffer size
	stacktraceBufSize = 1 * units.MiB

	metricsWALFaultCount = "fault-count"
	metricsWALFaultTime  = "fault-time"
)

type Agent struct {
	signalCh chan os.Signal

	shutdown    func()
	shutdownCtx context.Context

	pool *pgx.ConnPool

	metrics *cgm.CirconusMetrics

	walLock    sync.RWMutex
	lastWALLog string
	// timelineID is the timeline ID from the last queryLastLog()
	// operation. timelineID is protected by walLock.
	timelineID lsn.TimelineID

	walReadAhead     uint32
	maxConcurrentIOs uint

	// fdCache is a file descriptor cache to prevent re-open(2)'ing files
	// continually.
	fdCache gcache.Cache

	// ioReqCache is a read-through cache to:
	//
	// a) provide a reentrant interface
	// b) deduplicate page pread(2) requests (i.e. no thundering-herd for the same
	//    page file)
	// c) prevent tainting of the filesystem cache (i.e. ZFS ARC) by artificially
	//    promoting pages from the MRU to the MFU.
	// d) sized sufficiently large so that we can spend our time faulting in pages
	//    vs performing cache hits.
	ioReqCache gcache.Cache

	// walCache is a read-through cache to:
	//
	// a) provide a reentrant interface
	// b) deduplicate requests (i.e. no thundering-herd for the same WAL file)
	// c) deliberately intollerant of scans because we know the input is monotonic
	// d) sized to include only the walReadAhead
	walCache gcache.Cache
}

// _FDCacheValue is a wrapper value type that includes an RWMutex.
type _FDCacheValue struct {
	sync.RWMutex
	_RelationFile
	os.File
	isOpen bool
}

func New(cfg config.Config) (a *Agent, err error) {
	a = &Agent{
		walReadAhead: uint32(viper.GetInt(config.KeyWALReadAhead)),
	}

	switch mode := viper.GetString(config.KeyXLogMode); mode {
	case "xlog":
		xlogRE = xlogdumpRE.Copy()
	case "pg":
		xlogRE = pgXLogDumpRE.Copy()
	default:
		panic(fmt.Sprintf("unsupported %q mode: %q", config.KeyXLogMode, mode))
	}

	a.metrics, err = cgm.NewCirconusMetrics(cfg.Metrics)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create a stats agent")
	}
	// Emit a handful of constants to reflect what the state of this process is.
	a.metrics.SetTextValue(metricsVersionSelfCommit, buildtime.COMMIT)
	a.metrics.SetTextValue(metricsVersionSelfDate, buildtime.DATE)
	a.metrics.SetTextValue(metricsVersionSelfVersion, buildtime.VERSION)

	// Handle shutdown via a.shutdownCtx
	a.signalCh = make(chan os.Signal, 1)
	signal.Notify(a.signalCh, os.Interrupt, unix.SIGTERM, unix.SIGHUP, unix.SIGPIPE, unix.SIGINFO)

	a.shutdownCtx, a.shutdown = context.WithCancel(context.Background())

	{
		poolConfig := cfg.DBPool
		poolConfig.AfterConnect = func(conn *pgx.Conn) error {
			var version string
			sql := `SELECT VERSION()`
			if err := conn.QueryRowEx(a.shutdownCtx, sql, nil).Scan(&version); err != nil {
				return errors.Wrap(err, "unable to query DB version")
			}
			log.Debug().Uint32("backend-pid", conn.PID()).Str("version", version).Msg("established DB connection")
			a.metrics.SetTextValue(metricsDBVersionPG, version)

			return nil
		}

		if a.pool, err = pgx.NewConnPool(poolConfig); err != nil {
			return nil, errors.Wrap(err, "unable to create a new DB connection pool")
		}
	}

	{
		var numReservedFDs uint32 = 10
		var procNumFiles unix.Rlimit
		if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &procNumFiles); err != nil {
			return nil, errors.Wrap(err, "unable to determine rlimits for number of files")
		}
		maxNumOpenFiles := uint32(procNumFiles.Cur) - a.walReadAhead

		// fdCache is private to ioReqCache
		fdCacheSize := maxNumOpenFiles - numReservedFDs
		fdCacheTTL := 3600 * time.Second

		a.fdCache = gcache.New(int(fdCacheSize)).
			ARC().
			LoaderExpireFunc(func(key interface{}) (interface{}, *time.Duration, error) {
				rf, ok := key.(_RelationFile)
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
					_RelationFile: rf,
					File:          *f,
					isOpen:        true,
				}

				return val, &fdCacheTTL, nil
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
				a.metrics.Increment(metricsSysCloseCount)
			}).
			Build()
	}

	{
		// The ioCache is created before the walCache because the walCache feeds work
		// into the ioCache.
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
				rf, ok := key.(_RelationFile)
				if !ok {
					log.Panic().Msgf("unable to type assert key in IO Cache: %T %+v", key, key)
				}

				fRaw, err := a.fdCache.Get(rf)
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

				var buf [lsn.WALPageSize]byte
				pageNum, err := rf.PageNum()
				if err != nil {
					log.Warn().Err(err).Msgf("unable to find the page number: %+v", rf)
					return struct{}{}, nil, errors.Wrapf(err, "unable to find the page number: %+v", rf)
				}

				// FIXME(seanc@): Need to wrap this ReadAt() call in a wait group in
				// order to prevent IO starvation.
				start := time.Now()
				n, err := f.ReadAt(buf[:], pageNum*int64(lsn.WALPageSize))
				end := time.Now()

				a.metrics.RecordValue(metricsSysPreadLatency, float64(end.Sub(start)/time.Millisecond))
				a.metrics.Increment(metricsSysPreadCount)
				a.metrics.Add(metricsSysPreadCount, uint64(n))
				if err != nil {
					a.metrics.Increment(metricsXLogDumpErrorCount)
					log.Warn().Err(err).Str("database", rf.Database).Str("relation", rf.Relation).
						Str("block", rf.Block).Int64("page", pageNum).Int("n", n).Msg("error reading")
					return struct{}{}, nil, err
				}

				expire := ioReqCacheTTL

				return struct{}{}, &expire, nil
			}).
			Build()
	}

	// Deliberately use a scan-intolerant cache because the inputs are going to be
	// ordered.  When the cache is queried, return a faux result and actually
	// perform the real work in a background goroutine.
	a.walCache = gcache.New(2 * int(a.walReadAhead)).
		LRU().
		LoaderFunc(func(key interface{}) (interface{}, error) {
			defer func(start time.Time) {
				a.metrics.RecordValue(metricsWALFaultTime, float64(time.Now().Sub(start)/time.Second))
			}(time.Now())

			if err := a.prefaultWALFile(key.(string)); err != nil {
				return nil, errors.Wrapf(err, "unable to prefault WAL file %q", key.(string))
			}

			a.metrics.Increment(metricsWALFaultCount)
			return struct{}{}, nil
		}).
		Build()

	return a, nil
}

// Loop forever between two modes of operation: sleeping or primary, and a
// follower (the follower mode has its own criterium to figure out if it
// needs to do work).
func (a *Agent) Start() {
	var err error

	log.Debug().Msg("Starting " + buildtime.PROGNAME + " agent")

	go a.startSignalHandler()

	if viper.GetBool(config.KeyCirconusEnabled) {
		a.metrics.Start()
	}

	var dbState _DBState
	a.metrics.SetTextFunc(metricsDBState, func() string {
		return dbState.String()
	})
	defer a.metrics.RemoveTextFunc(metricsDBState)

	var loopImmediately bool = true
RETRY:
	for {
		if a.isShuttingDown() {
			break RETRY
		}

		if !loopImmediately {
			d := viper.GetDuration(config.KeyPGPollInterval)
			time.Sleep(d)
		}

		dbState, err = a.dbState()
		if err != nil {
			log.Error().Err(err).Msg("unable to determine if database is primary or not, retrying")
			loopImmediately = false
			goto RETRY
		}

		// FIXME(seanc@): Convert this from mode-specific methods to an interface
		// that is shared between a primary and follower interface.
		switch state := dbState; state {
		case _DBStatePrimary:
			loopImmediately = a.runPrimary()
		case _DBStateFollower:
			loopImmediately = a.runFollower()
		default:
			panic(fmt.Sprintf("unknown state: %+v", state))
		}
	}
}

// Stop cleans up and shuts down the Agent
func (a *Agent) Stop() {
	a.stopSignalHandler()
	a.shutdown()
	a.metrics.Flush()
	a.pool.Close()
	log.Debug().Msg("Stopped " + buildtime.PROGNAME + " agent")
}

// Wait blocks until shutdown
func (a *Agent) Wait() error {
	log.Debug().Msg("Starting wait")
	select {
	case <-a.shutdownCtx.Done():
	}

	return nil
}

// isShuttingDown is a convenience method that returns true when the agent is
// shutting down.
func (a *Agent) isShuttingDown() bool {
	select {
	case <-a.shutdownCtx.Done():
		return true
	default:
		return false
	}
}

// signalHandler runs the signal handler thread
func (a *Agent) startSignalHandler() {
	// pre-allocate a buffer
	buf := make([]byte, stacktraceBufSize)

	for {
		select {
		case <-a.shutdownCtx.Done():
			log.Debug().Msg("Shutting down")
			return
		case sig := <-a.signalCh:
			log.Info().Str("signal", sig.String()).Msg("Received signal")
			switch sig {
			case os.Interrupt, unix.SIGTERM:
				a.shutdown()
			case unix.SIGPIPE, unix.SIGHUP:
				// Noop
			case unix.SIGINFO:
				stacklen := runtime.Stack(buf, true)
				fmt.Printf("=== received SIGINFO ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
			default:
				panic(fmt.Sprintf("unsupported signal: %v", sig))
			}
		}
	}
}

// stopSignalHandler disables the signal handler
func (a *Agent) stopSignalHandler() {
	signal.Stop(a.signalCh)
}
