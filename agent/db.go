package agent

import (
	"fmt"

	"github.com/jackc/pgx"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/joyent/pg_prefaulter/lsn"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func connInit(conn *pgx.Conn) error {
	log.Debug().Msg("established DB connection")
	return nil
}

type _DBState int

const (
	unknown _DBState = iota
	primary
	follower
)

// dbState returns a constant indicating the state of the database
// (i.e. primary, follower).
func (a *Agent) dbState() (_DBState, error) {
	switch mode := viper.GetString(config.KeyMode); mode {
	case "primary":
		return primary, nil
	case "follower":
		return follower, nil
	case "auto":
		break
	default:
		panic(fmt.Sprintf("invalid mode: %q", mode))
	}

	tx, err := a.pool.BeginEx(a.shutdownCtx, nil)
	if err != nil {
		return unknown, errors.Wrap(err, "unable to determine if primary DB")
	}
	defer tx.RollbackEx(a.shutdownCtx)

	var inRecovery bool
	if err = tx.QueryRowEx(a.shutdownCtx, "SELECT pg_is_in_recovery()", nil).Scan(&inRecovery); err != nil {
		return unknown, errors.Wrap(err, "unable to execute primary check")
	}

	switch inRecovery {
	case true:
		return follower, nil
	case false:
		return primary, nil
	default:
		panic("what is logic?")
	}
}

type LSNQuery int

const (
	LastXLogReplayLocation LSNQuery = iota
	CurrentXLogFlushLocation
)

// queryLSN queries a specific LSN from the database.
func (a *Agent) queryLSN(lsnQuery LSNQuery) (lsn.LSN, error) {
	// sql must return one column with an LSN type as the result
	var sql string
	switch lsnQuery {
	case LastXLogReplayLocation:
		sql = "SELECT pg_last_xlog_replay_location()"
	case CurrentXLogFlushLocation:
		sql = "SELECT pg_current_xlog_flush_location()"
	default:
		panic(fmt.Sprintf("unsupported query: %v", lsnQuery))
	}

	tx, err := a.pool.BeginEx(a.shutdownCtx, nil)
	if err != nil {
		return lsn.InvalidLSN, errors.Wrap(err, "unable to begin transaction")
	}
	defer tx.RollbackEx(a.shutdownCtx)

	var lsnStr string
	err = tx.QueryRowEx(a.shutdownCtx, sql, nil).Scan(&lsnStr)
	if err != nil {
		return lsn.InvalidLSN, errors.Wrapf(err, "unable to query DB: %v", lsnQuery)
	}

	var l lsn.LSN
	if l, err = lsn.Parse(lsnStr); err != nil {
		return lsn.InvalidLSN, errors.Wrapf(err, "unable to parse LSN: %q", lsnStr)
	}

	return l, nil
}
