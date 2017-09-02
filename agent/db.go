package agent

import (
	"fmt"

	"github.com/jackc/pgx"
	"github.com/joyent/pg_prefaulter/lsn"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
)

func connInit(conn *pgx.Conn) error {
	log.Debug().Msg("established DB connection")
	return nil
}

// isDBPrimry returns true when
func (a *Agent) isDBPrimary() (bool, error) {
	tx, err := a.pool.BeginEx(a.shutdownCtx, nil)
	if err != nil {
		return false, errors.Wrap(err, "unable to determine if primary DB")
	}
	defer tx.RollbackEx(a.shutdownCtx)

	var inRecovery bool
	if err = tx.QueryRowEx(a.shutdownCtx, "SELECT pg_is_in_recovery()", nil).Scan(&inRecovery); err != nil {
		return false, errors.Wrap(err, "unable to execute primary check")
	}

	return !inRecovery, nil
}

type LSNQuery int

const (
	LastXLogReplayLocation LSNQuery = iota
	CurrentXLogFlushLocation
)

// QueryLSN queries a specific LSN from the database.
func (a *Agent) QueryLSN(lsnQuery LSNQuery) (lsn.LSN, error) {
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
