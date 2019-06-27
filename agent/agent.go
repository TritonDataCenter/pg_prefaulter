// Copyright © 2019 Joyent, Inc.
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
	"sync"
	"time"

	cgm "github.com/circonus-labs/circonus-gometrics"
	"github.com/jackc/pgx"
	"github.com/joyent/pg_prefaulter/agent/fhcache"
	"github.com/joyent/pg_prefaulter/agent/iocache"
	"github.com/joyent/pg_prefaulter/agent/metrics"
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
	pgConnCtx      context.Context
	pgConnShutdown func()
	pool           *pgx.ConnPool
	poolConfig     *config.DBPool
	lastWALLog     pg.WALFilename
	lastTimelineID pg.TimelineID

	fileHandleCache *fhcache.FileHandleCache
	ioCache         *iocache.IOCache
	walCache        *walcache.WALCache
	walTranslations *pg.WALTranslations
}

func New(cfg *config.Config) (a *Agent, err error) {
	a = &Agent{
		cfg: &cfg.Agent,
		walTranslations: &pg.WALTranslations{},
	}

	a.metrics, err = cgm.NewCirconusMetrics(cfg.Metrics)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create a stats agent")
	}
	// Emit a handful of constants to reflect what the state of this process is.
	a.metrics.SetTextValue(metrics.VersionSelfCommit, buildtime.COMMIT)
	a.metrics.SetTextValue(metrics.VersionSelfDate, buildtime.DATE)
	a.metrics.SetTextValue(metrics.VersionSelfVersion, buildtime.VERSION)

	a.setupSignals()

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
		walCache, err := walcache.New(a, a.shutdownCtx, cfg, a.metrics, a.ioCache, a.walTranslations)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize WAL cache")
		}

		a.walCache = walCache
	}

	return a, nil
}

// AcquireConnContext returns a Context used to signal when connections to
// PostgreSQL should be terminated.
func (a *Agent) AcquireConnContext() context.Context {
	a.pgStateLock.RLock()
	defer a.pgStateLock.RUnlock()

	return a.pgConnCtx
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
	go a.startDBStats()

	if viper.GetBool(config.KeyCirconusEnabled) {
		a.metrics.Start()
	}

	var dbState _DBState
	a.metrics.SetTextFunc(metrics.DBState, func() string {
		return dbState.String()
	})
	defer a.metrics.RemoveTextFunc(metrics.DBState)

	// The main event loop for the run command.  The run event loop runs through
	// the following six steps:
	//
	// 1) Shutdown if we've been told to shutdown.
	// 2) Sleep if we've been told to sleep in the previous iteration.
	// 3) Dump caches if a cache-invalidation event occurred.
	// 4) Determine version of postgres and translate WAL interactions
	// 5) Attempt to find WAL files.
	// 5a) Attempt to query the DB to find the WAL files.
	// 5b) Attempt to query the process args to find the WAL files.
	// 6) Fault pages in from the heap if we have found any WAL files.
	// 6a) Fault into PG using pg_prewarm() if detected.
	// 6b) Fault into the filesystem cache using pread(2) if pg_prewarm is not
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

		// 2) Sleep.  Sleep before purging the WALCache in order to allow processes
		//    in flight to complete.  If the sleep is not called before the purge,
		//    it's possible that an in-flight pg_xlogdump(1) would be cancelled
		//    before it completed a run.  This means that during an unexpected
		//    shutdown, FDs won't be closed for up to config.KeyPGPollInterval.
		if !sleepBetweenIterations {
			d := viper.GetDuration(config.KeyPGPollInterval)
			time.Sleep(d)
			sleepBetweenIterations = false
		}

		// 3) Dump cache. Calling Purge() on the WALCache purges all downstream
		//    caches (i.e. ioCache and fhCache).
		if purgeCache {
			a.resetPGConnCtx()
			a.walCache.Purge()
			purgeCache = false
		}

		// 4) Determine version of postgres and translate WAL interactions.
		if err := a.setWALTranslations(); err != nil {
			retry := handleErrors(err, "unable to translate WAL interactions")
			if retry {
				goto RETRY
			} else {
				break RETRY
			}
		}

		// 5) Get WAL files
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

		// 6) Fault in PostgreSQL heap pages identified in the WAL files
		if sleepBetweenIterations, err = a.prefaultWALFiles(walFiles); err != nil {
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

func (a *Agent) setWALTranslations() (error) {
	pgDataPath := viper.GetString(config.KeyPGData)
	pgVersion, err := a.getPostgresVersion(pgDataPath)

	if err != nil {
		return newVersionError(err, true)
	}

	*a.walTranslations = pg.Translate(pgVersion)

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

	a.metrics.SetGauge(metrics.WALFileCandidate, len(walFiles))

	return walFiles, nil
}

// prefaultWALFiles pre-faults the heap pages referenced in WAL files.  When
// moreWork is set to true it indicates the caller should loop immediately.
func (a *Agent) prefaultWALFiles(walFiles pg.WALFiles) (moreWork bool, err error) {
	uniqueWALFiles := walFiles.Unique()

	// Read through the cache to prefault a given WAL file.  The cache
	// begins to fault the WAL file as soon as requested in the event of
	// a cache miss.  FaultWALFile() dedupes requests and prevents a WAL
	// file from concurrent prefault operations.
	waitWALFiles := make(pg.WALFiles, 0, len(walFiles))
	for _, walFile := range uniqueWALFiles {
		if faulting, _ := a.walCache.FaultWALFile(walFile); faulting {
			waitWALFiles = append(waitWALFiles, walFile)
		}
	}

	return len(waitWALFiles) > pg.NumOldLSNs, nil
}

// resetPGConnCtx resets the PostgreSQL connection context.
func (a *Agent) resetPGConnCtx() {
	a.pgStateLock.Lock()
	defer a.pgStateLock.Unlock()

	a.pgConnShutdown()
	a.pgConnCtx, a.pgConnShutdown = context.WithCancel(a.shutdownCtx)
}

// stopSignalHandler disables the signal handler
func (a *Agent) stopSignalHandler() {
	signal.Stop(a.signalCh)
}
