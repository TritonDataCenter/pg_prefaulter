package agent

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"time"

	"golang.org/x/sys/unix"

	"github.com/alecthomas/units"
	"github.com/jackc/pgx"
	"github.com/joyent/pg_prefaulter/config"
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
}

func New(cfg config.Config) (a *Agent, err error) {
	a = &Agent{}

	{
		poolConfig := cfg.ConnPoolConfig
		poolConfig.AfterConnect = connInit
		if a.pool, err = pgx.NewConnPool(poolConfig); err != nil {
			return nil, errors.Wrap(err, "unable to create a new DB connection pool")
		}
	}

	// Handle shutdown via a.shutdownCtx
	a.signalCh = make(chan os.Signal, 1)
	signal.Notify(a.signalCh, os.Interrupt, unix.SIGTERM, unix.SIGHUP, unix.SIGPIPE, unix.SIGINFO)

	a.shutdownCtx, a.shutdown = context.WithCancel(context.Background())

	return a, nil
}

// Loop forever between two modes of operation: sleeping or primary, and a
// follower (the follower mode has its own criterium to figure out if it
// needs to do work).
func (a *Agent) Start() {
	var err error

	log.Debug().Msg("Starting agent")

	go a.startSignalHandler()

	for i := 0; i < 3; i++ { // FIXME(seanc@): Should loop infinitely
		var loopImmediately bool

		var isPrimary bool
		switch mode := viper.GetString(config.KeyMode); mode {
		case "auto":
			isPrimary, err = a.isDBPrimary()
		case "primary":
			isPrimary = true
		case "follower":
			isPrimary = false
		default:
			log.Error().Str("mode", mode).Msg("invalid mode, defaulting to auto")
			isPrimary, err = a.isDBPrimary()
		}

		switch {
		case err != nil:
			log.Error().Err(err).Msg("unable to query the primary")
			loopImmediately = false
		case isPrimary:
			loopImmediately = a.runPrimary()
		default:
			loopImmediately = a.runFollower()
		}

		if !loopImmediately {
			d := viper.GetDuration(config.KeyPollInterval)
			log.Debug().Dur("sleep duration", d).Msg("sleeping before next poll")
			time.Sleep(d)
		}
	}

	// FIXME(seanc@): until the main logic is complete, shutdown prematurely
	a.Stop()
}

// Stop cleans up and shuts down the Agent
func (a *Agent) Stop() {
	a.stopSignalHandler()
	a.shutdown()
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

// runFollower is excuted when talking to a readonly follower.  When returning
// true, we're requesting an immediately loop without any pause between
// iterations.
func (a *Agent) runFollower() (loopImmediately bool) {
	log.Debug().Msg("follower")
	tx, err := a.pool.BeginEx(a.shutdownCtx, nil)
	if err != nil {
		log.Error().Err(err).Msg("unable to begin transaction")
		return false
	}
	defer tx.RollbackEx(a.shutdownCtx)

	lsn, err := a.QueryLSN(LastXLogReplayLocation)
	if err != nil {
		log.Error().Err(err).Msg("unable to query LSN")
		return false
	}

	log.Debug().Str("lsn", lsn.String()).Str("wal", lsn.WALFileName()).Msg("")
	return true
}

// runPrimary is executed when talking to a writable database.
func (a *Agent) runPrimary() (loopImmediately bool) {
	log.Debug().Msg("primary")
	return false
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
		case os.Interrupt:
			a.shutdown()
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
