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
	cgm "github.com/circonus-labs/circonus-gometrics"
	"github.com/jackc/pgx"
	"github.com/joyent/pg_prefaulter/agent/fhcache"
	"github.com/joyent/pg_prefaulter/agent/iocache"
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

	pgStateLock sync.RWMutex
	pool        *pgx.ConnPool
	poolConfig  *config.DBPool
	lastWALLog  string
	// timelineID is the timeline ID from the last queryLastLog()
	// operation. timelineID is protected by walLock.
	timelineID pg.TimelineID

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

	log.Debug().Msg("Starting " + buildtime.PROGNAME + " agent")

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

	var loopImmediately bool = true
RETRY:
	for {
		if lib.IsShuttingDown(a.shutdownCtx) {
			break RETRY
		}

		if !loopImmediately {
			d := viper.GetDuration(config.KeyPGPollInterval)
			time.Sleep(d)
		}

		// If the connection pool is uninitialized or failed to acquire a connection
		// at startup, retry assuming this is a transient error.  ensureDBPool
		// guards all future calls in the event loop from a nil pool.
		if err := a.ensureDBPool(); err != nil {
			if a.cfg.RetryInit {
				log.Error().Err(err).Msg("unable to initialize db connection pool, retrying")
				loopImmediately = false
				goto RETRY
			} else {
				log.Error().Err(err).Msg("unable to initialize db connection pool, exiting")
				a.shutdown()
				break RETRY
			}
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
			loopImmediately, err = a.runPrimary()
			if err != nil {
				log.Error().Err(err).Msg("unable to run primary")
			}
		case _DBStateFollower:
			loopImmediately, err = a.runFollower()
			if err != nil {
				log.Error().Err(err).Msg("unable to run follower")
			}
		default:
			panic(fmt.Sprintf("unknown state: %+v", state))
		}

		// Purge all the things if we can't talk to PG.  Calling Purge() on the
		// WALCache purges all downstream caches (i.e. iocache and fhcache).
		if err != nil {
			a.walCache.Purge()
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

// stopSignalHandler disables the signal handler
func (a *Agent) stopSignalHandler() {
	signal.Stop(a.signalCh)
}
