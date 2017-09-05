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
}

func New(cfg config.Config) (a *Agent, err error) {
	a = &Agent{}

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

	return a, nil
}

// Loop forever between two modes of operation: sleeping or primary, and a
// follower (the follower mode has its own criterium to figure out if it
// needs to do work).
func (a *Agent) Start() {
	var err error

	log.Info().Msg("Starting " + buildtime.PROGNAME + " agent")

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

// runFollower is excuted when talking to a readonly follower.  When returning
// true, we're requesting an immediately loop without any pause between
// iterations.
func (a *Agent) runFollower() (loopImmediately bool) {
	_, err := a.queryLag(_QueryLagFollower)
	if err != nil {
		log.Error().Err(err).Msg("unable to query follower lag")
		return false
	}

	err = a.queryLastLog()
	if err != nil {
		log.Error().Err(err).Msg("unable to query last WAL lag")
		return false
	}

	lsn, err := a.queryLSN(LastXLogReplayLocation)
	if err != nil {
		log.Error().Err(err).Msg("unable to query LSN")
		return false
	}

	log.Debug().Str("lsn", lsn.String()).Str("wal", lsn.WALFileName()).Msg("")
	return false // FIXME(seanc@): Should return true if we need to loop immediately
}

// signalHandler runs the signal handler thread
func (a *Agent) startSignalHandler() {
	// pre-allocate a buffer
	buf := make([]byte, stacktraceBufSize)

	select {
	case <-a.shutdownCtx.Done():
		log.Debug().Msg("Shutting down")
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

// stopSignalHandler disables the signal handler
func (a *Agent) stopSignalHandler() {
	signal.Stop(a.signalCh)
}
