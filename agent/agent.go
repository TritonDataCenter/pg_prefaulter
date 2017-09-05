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
	walCache   gcache.Cache
	walCacheWG sync.WaitGroup
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

	if err := a.initDBPool(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to initialize db connection pool")
	}

	if err := a.initFDCache(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to initialize fdcache")
	}

	// The ioCache is created before the walCache because the walCache feeds work
	// into the ioCache.
	if err := a.initIOReqCache(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to initialize ioReqCache")
	}

	// Initialize the walCache last because this is the primary driver of work in
	// the tier of caches (i.e. WAL -> IO -> FD).
	if err := a.initWALCache(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to initialize WAL cache")
	}

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

	// Drain work from the WAL cache before returning
	a.walCacheWG.Wait()

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
