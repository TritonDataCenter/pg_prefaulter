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
	"github.com/joyent/pg_prefaulter/agent/fhcache"
	"github.com/joyent/pg_prefaulter/agent/iocache"
	"github.com/joyent/pg_prefaulter/agent/proc"
	"github.com/joyent/pg_prefaulter/agent/walcache"
	"github.com/joyent/pg_prefaulter/buildtime"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/joyent/pg_prefaulter/lib"
	"github.com/joyent/pg_prefaulter/pg"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type Agent struct {
	cfg *config.Agent

	signalCh chan os.Signal

	shutdown    func()
	shutdownCtx context.Context

	metrics *cgm.CirconusMetrics

	// pgStateLock protects the following values.  lastWALLog and lastTimelineID
	// are the WAL filename and timeline ID from previous call to queryLastLog()
	// operation.
	pgStateLock    sync.RWMutex
	pool           *pgx.ConnPool
	poolConfig     *config.DBPool
	lastWALLog     pg.WALFilename
	lastTimelineID pg.TimelineID

	fileHandleCache *fhcache.FileHandleCache
	ioCache         *iocache.IOCache
	walCache        *walcache.WALCache
}

func New(cfg *config.Config) (a *Agent, err error) {
	a = &Agent{
		cfg: &cfg.Agent,
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
	a.signalCh = make(chan os.Signal, 10)
	signal.Notify(a.signalCh, os.Interrupt, unix.SIGTERM, unix.SIGHUP, unix.SIGPIPE, unix.SIGINFO)

	a.shutdownCtx, a.shutdown = context.WithCancel(context.Background())

	if err := a.initDBPool(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to initialize db connection pool")
	}

	{
		fhCache, err := fhcache.New(a.shutdownCtx, cfg, a.metrics)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize filehandle cache")
		}

		a.fileHandleCache = fhCache
	}

	{
		ioCache, err := iocache.New(a.shutdownCtx, cfg, a.metrics, a.fileHandleCache)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize IO Cache")
		}

		a.ioCache = ioCache
	}

	{
		walCache, err := walcache.New(a.shutdownCtx, cfg, a.metrics, a.ioCache)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize WAL cache")
		}

		a.walCache = walCache
	}

	return a, nil
}

// Loop forever between two modes of operation: sleeping or primary, and a
// follower (the follower mode has its own criterium to figure out if it
// needs to do work).
func (a *Agent) Start() {
	var err error

	log.Info().Str("date", buildtime.DATE).
		Str("version", buildtime.VERSION).
		Str("commit", buildtime.COMMIT).
		Str("tag", buildtime.TAG).
		Msg("Starting " + buildtime.PROGNAME + " agent")

	a.pgStateLock.Lock()
	if a.pool != nil {
		// In the event that the caller cycles between a Start()'ed and Stop()'ed
		// agent, reset the DB connection pool.  The DB connection pool is stopped
		// during Stop() but is not nil'ed out in order to let DB connections drain
		// asynchronously after Stop() has been called.
		a.pool = nil
	}
	a.pgStateLock.Unlock()

	go a.handleSignals()

	if viper.GetBool(config.KeyCirconusEnabled) {
		a.metrics.Start()
	}

	var dbState _DBState
	a.metrics.SetTextFunc(metricsDBState, func() string {
		return dbState.String()
	})
	defer a.metrics.RemoveTextFunc(metricsDBState)

	// The main event loop for the run command.  The run event loop runs through
	// the following six steps:
	//
	// 1) Shutdown if we've been told to shutdown.
	// 2) Dump caches if a cache-invalidation event occurred.
	// 3) Sleep if we've been told to sleep in the previous iteration.
	// 4) Attempt to find WAL files.
	// 4a) Attempt to query the DB to find the WAL files.
	// 4b) Attempt to query the process args to find the WAL files.
	// 5) Fault pages in from the heap if we have found any WAL files.
	// 5a) Fault into PG using pg_prewarm() if detected.
	// 5b) Fault into the filesystem cache using pread(2) if pg_prewarm is not
	//     available.

	sleepBetweenIterations := true
	var purgeCache bool

	// Use a closure to reduce the boiler plate in controlling the event loop.
	handleErrors := func(rawErr error, msg string) (retry bool) {
		// FIXME(seanc@): BFS traverse rawErr to find the following error types.
		// This could/should be errwrap.Contains().
		if err, ok := rawErr.(retryableError); ok {
			retry = err.retry()
		}

		if err, ok := rawErr.(purgeEventError); ok {
			purgeCache = err.purgeCache()
		}

		switch {
		case retry && a.cfg.RetryInit:
			log.Error().Err(rawErr).Str("next step", "retrying").Msg(msg)
			sleepBetweenIterations = false
			return true
		default:
			log.Error().Err(rawErr).Str("next step", "exiting").Msg(msg)
			a.shutdown()
			return false
		}
	}
RETRY:
	for {
		// 1) Shutdown
		if lib.IsShuttingDown(a.shutdownCtx) {
			break RETRY
		}

		// 2) Dump cache. Calling Purge() on the WALCache purges all downstream
		//    caches (i.e. ioCache and fhCache).
		if purgeCache {
			a.walCache.Purge()
			purgeCache = false
		}

		// 3) Sleep
		if !sleepBetweenIterations {
			d := viper.GetDuration(config.KeyPGPollInterval)
			time.Sleep(d)
			sleepBetweenIterations = false
		}

		// 4) Get WAL files
		var walFiles []pg.WALFilename
		walFiles, err = a.getWALFiles()
		if err != nil {
			retry := handleErrors(err, "unable to find WAL files")
			if retry {
				goto RETRY
			} else {
				break RETRY
			}
		}

		// 5) Fault in PostgreSQL heap pages identified in the WAL files
		if err = a.prefaultWALFiles(walFiles); err != nil {
			retry := handleErrors(err, "unable to prefault WAL files")
			if retry {
				goto RETRY
			} else {
				break RETRY
			}
		}
	}
}

// Stop cleans up and shuts down the Agent
func (a *Agent) Stop() {
	a.stopSignalHandler()
	a.shutdown()
	a.metrics.Flush()

	a.pgStateLock.Lock()
	defer a.pgStateLock.Unlock()
	if a.pool != nil {
		a.pool.Close()

		// NOTE(seanc): explicitly do not nil out the pool value because the
		// connection pool does the right thing(tm) with regards to preventing new
		// connections from being established.  Agent.Start() will reset the value
		// to a nil value.
		//a.pool = nil
	}

	log.Debug().Msg("Stopped " + buildtime.PROGNAME + " agent")
}

// Wait blocks until shutdown
func (a *Agent) Wait() error {
	log.Debug().Msg("Starting wait")
	select {
	case <-a.shutdownCtx.Done():
	}

	// Drain work from the WAL cache before returning
	a.walCache.Wait()

	return nil
}

// getWALFiles returns a list of WAL files to be processed for prefaulting.
// getWALFiles attempts to connect to the database to find the active WAL file
// being applied.  If the database is starting up and can not accept new
// connections, attempt to extract the WAL file from the process args.  If the
// database socket is unavailable, do nothing and do not attempt to process the
// ps(1) args.
//
// FIXME(seanc@): Create a WALFaulter interface that can be DB-backed or
// process-arg backed.
func (a *Agent) getWALFiles() (pg.WALFiles, error) {
	// Rely on getWALFilesDB() or getWALFilesProcArgs() to update this value
	a.metrics.SetTextValue(proc.MetricsWALLookupMode, "error")

	var dbErr error
	var walFiles pg.WALFiles
	walFiles, dbErr = a.getWALFilesDB()
	if dbErr == nil {
		return walFiles, nil
	}

	// Under the following errors, we attempt to parse ps(1)'s args:
	//
	// See PostgreSQL's src/backend/utils/errcodes.txt for additional error codes.
	var processPSArgs bool
	if dbErr != nil {
		switch pgErr := errors.Cause(dbErr).(type) {
		case pgx.PgError:
			switch {
			case pgErr.Code == "57P03" &&
				(pgErr.Message == "the database system is starting up" ||
					pgErr.Message == "the database system is in recovery mode" ||
					pgErr.Message == "the database system is shutting down"):
				// Help the system along when it's starting up or in recovery
				processPSArgs = true
			case pgErr.Code == "53300" &&
				(pgErr.Message == "sorry, too many clients already" ||
					pgErr.Message == "remaining connection slots are reserved for non-replication superuser connections"):
				// Help the system along when there are too many connections under the
				// assumption that a connection slot will open up in the future.
				processPSArgs = true
			case pgErr.Code == "42501" && pgErr.Message == "must be superuser to connect during database shutdown":
				// During a shutdown continue to apply WAL files up until the bitter end
				// in order to help improve performance when the database restarts.
				processPSArgs = true
			default:
				// An unknown PG error happened and we were unable to connect.  Assume
				// that this is a permanent failure and we need to get attention
				// (e.g. wrong credentials).
				return nil, newWALError(pgErr, false, false)
			}
		default:
			// An unknown error happened (not a PgError) and we were unable to fetch the
			// WAL files.  Assume PG has disappeared out from under us.  In response to
			// this, dump cache and attempt to retry.
			return nil, newWALError(dbErr, true, true)
		}
	}

	if processPSArgs {
		var psErr error
		walFiles, psErr = a.getWALFilesProcArgs()
		if psErr != nil {
			// Return a retriable error since processPSArgs returned true indicating
			// the database was starting up.
			raisedErr := fmt.Errorf("unable to query the DB (%+v) or process arguments (%+v)", dbErr, psErr)
			return nil, newWALError(raisedErr, true, true)
		}
	}

	a.metrics.SetGauge(metricsWALFileCandidate, len(walFiles))

	return walFiles, nil
}

// handleSignals runs the signal handler thread
func (a *Agent) handleSignals() {
	const stacktraceBufSize = 1 * units.MiB

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

// prefaultWALFiles pre-faults the heap pages referenced in WAL files.  When
// moreWork is set to true it indicates the caller should loop immediately.
	// 1) Read through the cache to prefault a given WAL file.  The cache takes
	//    the lookup request and begins to fault the WAL file as soon as we
	//    request it in the event of a cache miss.  The cache dedupes requests and
	//    prevents a WAL file from being prefaulted a second time (until the
	//    caches are purged)..
	// 2) Perform all cache lookups using GetIFPresent() in order to trigger a
	//    backfill of the entry.  GetIFPresent() has a side-effect of launching
	//    the LoaderFunc(), which will populate the cache and deduplicate requests
	//    if the cache hasn't been filled by the subsequent iteration through the
	//    cache.  If all entries were found in the cache, sleep.  If we had any
	//    cache misses loop immediately.
	for _, walFile := range walFiles {
		_, err := a.walCache.GetIFPresent(walFile)
		if err == gcache.KeyNotFoundError {
			moreWork = true
func (a *Agent) prefaultWALFiles(walFiles pg.WALFiles) (moreWork bool, err error) {
		}
	}

	// If we had a single cache miss previously, perform the exact same lookups a
	// second time, but this time with a blocking call to Get().  We perform this
	// second loop through the cache in order to limit the amount of activity and
	// let the dispatched work run to completion before attempting to process
	// additional WAL files.
	if moreWork {
		for _, walFile := range walFiles {
			if _, err := a.walCache.Get(walFile); err != nil {
				return false, errors.Wrap(err, "unable to perform synchronous Get operation on WAL file cache")
			}
		}
	}

	return moreWork, nil
}

// stopSignalHandler disables the signal handler
func (a *Agent) stopSignalHandler() {
	signal.Stop(a.signalCh)
}
