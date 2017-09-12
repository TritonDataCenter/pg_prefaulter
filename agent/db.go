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
	"fmt"
	"math"

	"github.com/jackc/pgx"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/joyent/pg_prefaulter/pg"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

const (
	metricsDBConnectionStateName = "connected"
	metricsDBLagDurability       = "durability"
	metricsDBLagDurabilityBytes  = "durability-lag"
	metricsDBLagFlush            = "flush"
	metricsDBLagFlushBytes       = "flush-lag"
	metricsDBLagVisibility       = "visibility"
	metricsDBLagVisibilityBytes  = "visibility-lag"
	metricsDBLastTransaction     = "last-transaction_ms"
	metricsDBLastTransactionAge  = "last-transaction-age_ms"
	metricsDBPeerSyncState       = "peer-sync-state"
	metricsDBSenderState         = "sender-state"
	metricsDBState               = "db-state"
	metricsDBTimelineID          = "timeline-id"
	metricsDBVersionPG           = "version-pg"
	metricsDBWALCount            = "num-wal-files"
	metricsWALFileCandidate      = "num-wal-candidates"
	metricsVersionSelfCommit     = "version-self-commit"
	metricsVersionSelfDate       = "version-self-date"
	metricsVersionSelfVersion    = "version-self-version"
)

type (
	_DBConnectionState int
)

const (
	_DBConnectionStateUnknown _DBConnectionState = iota
	_DBConnectionStateConnected
	_DBConnectionStateDisconnected
)

func (s _DBConnectionState) String() string {
	switch s {
	case _DBConnectionStateUnknown:
		return "unknown connection state"
	case _DBConnectionStateDisconnected:
		return "disconnected"
	case _DBConnectionStateConnected:
		return "connected"
	default:
		panic(fmt.Sprintf("unknown connection state: %d", s))
	}
}

type _DBState int

const (
	_DBStateUnknown _DBState = iota
	_DBStatePrimary
	_DBStateFollower
)

func (s _DBState) String() string {
	switch s {
	case _DBStateUnknown:
		return "unknown"
	case _DBStatePrimary:
		return "primary"
	case _DBStateFollower:
		return "follower"
	default:
		panic(fmt.Sprintf("unknown state: %d", s))
	}
}

// dbState returns a constant indicating the state of the database
// (i.e. primary, follower).
func (a *Agent) dbState() (_DBState, error) {
	switch mode := viper.GetString(config.KeyPGMode); mode {
	case "primary":
		return _DBStatePrimary, nil
	case "follower":
		return _DBStateFollower, nil
	case "auto":
		break
	default:
		panic(fmt.Sprintf("invalid mode: %q", mode))
	}

	var inRecovery bool
	if err := a.pool.QueryRowEx(a.shutdownCtx, "SELECT pg_is_in_recovery()", nil).Scan(&inRecovery); err != nil {
		return _DBStateUnknown, errors.Wrap(err, "unable to execute primary check")
	}

	switch inRecovery {
	case true:
		return _DBStateFollower, nil
	case false:
		return _DBStatePrimary, nil
	default:
		panic("what is logic?")
	}
}

func (a *Agent) initDBPool(cfg *config.Config) (err error) {
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
		return errors.Wrap(err, "unable to create a new DB connection pool")
	}

	return nil
}

type _QueryLag int

const (
	_QueryLagUnknown _QueryLag = iota
	_QueryLagPrimary
	_QueryLagFollower
)

// queryLag queries the database for its understanding of lag.
func (a *Agent) queryLag(lagQuery _QueryLag) (uint64, error) {
	const unknownLag = math.MaxUint64

	// Log whether or not we're connected or not
	var connectedState _DBConnectionState
	defer func() { a.metrics.SetTextValue(metricsDBConnectionStateName, connectedState.String()) }()

	// sql must return one column with an LSN type as the result
	var sql string
	switch lagQuery {
	case _QueryLagPrimary:
		sql = `SELECT state, sync_state, (pg_xlog_location_diff(sent_location, write_location))::FLOAT8 AS durability_lag_bytes, (pg_xlog_location_diff(sent_location, flush_location))::FLOAT8 AS flush_lag_bytes, (pg_xlog_location_diff(sent_location, replay_location))::FLOAT8 AS visibility_lag_bytes, COALESCE(EXTRACT(EPOCH FROM '0'::INTERVAL), 0.0)::FLOAT8 AS last_commit_age FROM pg_stat_replication ORDER BY visibility_lag_bytes`
	case _QueryLagFollower:
		sql = `SELECT 'receiving' AS state, 'applying' AS sync_state, 0.0::FLOAT8 AS durability_lag_bytes, 0.0::FLOAT8 AS flush_lag_bytes, COALESCE((pg_xlog_location_diff(pg_last_xlog_receive_location(), pg_last_xlog_replay_location()))::FLOAT8, 0.0)::FLOAT8 AS visibility_lag_bytes, COALESCE(EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp())::INTERVAL), 0.0)::FLOAT8 AS last_commit_age`
	default:
		panic(fmt.Sprintf("unsupported query: %v", lagQuery))
	}

	var err error
	var rows *pgx.Rows
	rows, err = a.pool.QueryEx(a.shutdownCtx, sql, nil)
	if err != nil {
		return unknownLag, errors.Wrapf(err, "unable to query lag: %v", lagQuery)
	}
	defer rows.Close()

	var senderState, syncState string
	var durabilityLag, flushLag, visibilityLag, txnAge float64 = math.NaN(), math.NaN(), math.NaN(), math.NaN()
	var numRows int
	for rows.Next() {
		err = rows.Scan(&senderState, &syncState, &durabilityLag, &flushLag, &visibilityLag, &txnAge)
		if err != nil {
			return unknownLag, errors.Wrap(err, "unable to scan lag response")
		}

		numRows++

		// Only record values that actually change.  Don't record metrics that are
		// missing on a shard member.
		switch lagQuery {
		case _QueryLagPrimary:
			a.metrics.RecordValue(metricsDBLagDurability, durabilityLag)
			a.metrics.Gauge(metricsDBLagDurabilityBytes, durabilityLag)
			a.metrics.RecordValue(metricsDBLagFlush, flushLag)
			a.metrics.Gauge(metricsDBLagFlushBytes, flushLag)
		case _QueryLagFollower:
			a.metrics.RecordValue(metricsDBLastTransaction, txnAge)
			a.metrics.Gauge(metricsDBLastTransactionAge, txnAge)
		}

		a.metrics.RecordValue(metricsDBLagVisibility, visibilityLag)
		a.metrics.Gauge(metricsDBLagVisibilityBytes, visibilityLag)
		a.metrics.SetTextValue(metricsDBPeerSyncState, syncState)
		a.metrics.SetTextValue(metricsDBSenderState, senderState)
	}

	if rows.Err() != nil {
		return unknownLag, errors.Wrap(err, "unable to process lag")
	}

	if numRows == 0 {
		connectedState = _DBConnectionStateDisconnected
	} else {
		connectedState = _DBConnectionStateConnected
	}

	return uint64(visibilityLag), nil
}

// queryLastLog queries to see if the WAL log for a given server has changed.
func (a *Agent) queryLastLog() (pg.TimelineID, error) {
	sql := "SELECT timeline_id, redo_wal_file FROM pg_control_checkpoint()"

	rows, err := a.pool.QueryEx(a.shutdownCtx, sql, nil)
	if err != nil {
		return 0, errors.Wrapf(err, "unable to query last WAL log")
	}
	defer rows.Close()

	var numWALFiles uint64
	defer func() { a.metrics.Add(metricsDBWALCount, numWALFiles) }()

	var timelineID pg.TimelineID
	var walFile string
	for rows.Next() {
		err = rows.Scan(&timelineID, &walFile)
		if err != nil {
			return 0, errors.Wrap(err, "unable to scan WAL file")
		}

		a.pgStateLock.Lock()
		defer a.pgStateLock.Unlock()
		if a.lastWALLog != "" && a.lastWALLog != walFile {
			numWALFiles++
		}
		a.lastWALLog = walFile

		// If the timeline changed, purge the walCache assuming we're going to need
		// to prefault in new data.
		if a.timelineID != 0 && a.timelineID != timelineID {
			a.walCache.Purge()
		}
		a.timelineID = timelineID
	}

	if rows.Err() != nil {
		return 0, errors.Wrap(err, "unable to process WAL lag")
	}

	a.metrics.Set(metricsDBTimelineID, uint64(timelineID))
	return timelineID, nil
}

type LSNQuery int

const (
	LastXLogReplayLocation LSNQuery = iota
	CurrentXLogFlushLocation
)

// queryLSN queries a specific LSN from the database.
func (a *Agent) queryLSN(lsnQuery LSNQuery) (l pg.LSN, err error) {
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

	var lsnStr string
	if err := a.pool.QueryRowEx(a.shutdownCtx, sql, nil).Scan(&lsnStr); err != nil {
		return pg.InvalidLSN, errors.Wrapf(err, "unable to query DB: %v", lsnQuery)
	}

	if l, err = pg.ParseLSN(lsnStr); err != nil {
		return pg.InvalidLSN, errors.Wrapf(err, "unable to parse LSN: %q", lsnStr)
	}

	return l, nil
}
