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
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"strconv"
	"time"

	"github.com/alecthomas/units"
	"github.com/jackc/pgx"
	"github.com/joyent/pg_prefaulter/agent/proc"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/joyent/pg_prefaulter/pg"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
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
		dbStats.dbState.Set("follower")
		return _DBStateFollower, nil
	case false:
		dbStats.dbState.Set("primary")
		return _DBStatePrimary, nil
	default:
		panic("what is logic?")
	}
}

// ensureDBPool creates a new database connection pool.  If the connection fails
// to be established, ensureDBPool will return an error.
func (a *Agent) ensureDBPool() (err error) {
	a.pgStateLock.RLock()
	if a.pool != nil {
		a.pgStateLock.RUnlock()
		return nil
	}
	a.pgStateLock.RUnlock()

	a.pgStateLock.Lock()
	if a.pool != nil {
		a.pgStateLock.Unlock()
		return nil
	}
	defer a.pgStateLock.Unlock()

	var pool *pgx.ConnPool
	if pool, err = pgx.NewConnPool(*a.poolConfig); err != nil {
		return errors.Wrap(err, "unable to create a new DB connection pool")
	}

	a.pool = pool
	return nil
}

// findPostgreSQLPostmasterPID looks on the filesystem to find PostgreSQL's PID.
func (a *Agent) findPostgreSQLPostmasterPID() (pid proc.PID, err error) {
	const errValue = 0
	buf, err := ioutil.ReadFile(a.cfg.PostgreSQLPIDPath)
	if err != nil {
		return errValue, errors.Wrap(err, "unable to read PostgreSQL postmaster PID file")
	}

	scanner := bufio.NewScanner(bytes.NewReader(buf))
	var pidStr string
	for scanner.Scan() {
		pidStr = scanner.Text()
		break
	}

	if err := scanner.Err(); err != nil {
		return errValue, errors.Wrap(err, "unable to extract PostgreSQL's PID number")
	}

	pid64, err := strconv.ParseUint(pidStr, 10, 64)
	if err != nil {
		return errValue, errors.Wrap(err, "unable to parse PostgreSQL PID number")
	}

	return proc.PID(pid64), nil
}

// getWALFilesDB returns a list of WAL files according to PostgreSQL
func (a *Agent) getWALFilesDB() (pg.WALFiles, error) {
	if err := a.ensureDBPool(); err != nil {
		return nil, errors.Wrap(err, "unable to get WAL db files")
	}

	timelineID, oldLSNs, err := pg.QueryOldestLSNs(a.shutdownCtx, a.pool, a.walCache)
	if err != nil {
		return nil, errors.Wrap(err, "unable to query PostgreSQL checkpoint information")
	}

	var numWALFiles uint64
	defer func() {
		a.metrics.Add(metricsDBWALCount, numWALFiles)
		dbStats.numWALFiles.Set(int64(numWALFiles))
	}()

	func() {
		// If the timeline changed, purge the walCache assuming we're going to need
		// to prefault in new heap data.
		a.pgStateLock.Lock()
		defer a.pgStateLock.Unlock()
		if a.lastTimelineID != timelineID {
			if a.lastTimelineID != 0 {
				a.walCache.Purge()
			}
			a.lastTimelineID = timelineID
		}
		a.metrics.Set(metricsDBTimelineID, uint64(timelineID))
		dbStats.timelineID.Set(int64(timelineID))
	}()

	walFiles := make(pg.WALFiles, 0, len(oldLSNs))
	for _, oldLSN := range oldLSNs {
		walFile := oldLSN.WALFilename(timelineID)
		func() {
			a.pgStateLock.Lock()
			defer a.pgStateLock.Unlock()

			if a.lastWALLog != walFile {
				if a.lastWALLog != "" {
					// Only increment the counter once we've initialized ourself to have a
					// last log
					numWALFiles++
				}
				a.lastWALLog = walFile
			}
		}()

		predictedWALFiles, err := a.predictDBWALFilenames(walFile)
		if err != nil {
			log.Debug().Err(err).
				Str("walfile", string(walFile)).
				Msg("unable to predict DB WAL filenames")
			continue
		}
		walFiles = append(walFiles, predictedWALFiles...)
	}

	a.metrics.SetTextValue(proc.MetricsWALLookupMode, "db")
	dbStats.numWALFiles.Set(int64(len(walFiles)))

	return walFiles, nil
}

// initDBPool configures the database connection pool for lazy initialization.
// The database connection pool won't be initialized until ensureDBPool is
// called.
func (a *Agent) initDBPool(cfg *config.Config) (err error) {
	cfg.DBPool.AfterConnect = func(conn *pgx.Conn) error {
		var version string
		sql := `SELECT VERSION()`
		if err := conn.QueryRowEx(a.shutdownCtx, sql, nil).Scan(&version); err != nil {
			return errors.Wrap(err, "unable to query DB version")
		}
		log.Debug().Uint32("backend-pid", conn.PID()).Str("version", version).Msg("established DB connection")
		a.metrics.SetTextValue(metricsDBVersionPG, version)

		return nil
	}

	a.poolConfig = &cfg.DBPool

	return nil
}

type _QueryLag int

const (
	_QueryLagUnknown _QueryLag = iota
	_QueryLagPrimary
	_QueryLagFollower
)

// queryLag queries the database for its understanding of lag.
func (a *Agent) queryLag(lagQuery _QueryLag) (units.Base2Bytes, error) {
	// FIXME(seanc@): units.Base2Bytes is an int64
	const unknownLag = units.Base2Bytes(math.MaxInt64)

	// Log whether or not we're connected or not
	var connectedState _DBConnectionState
	defer func() { a.metrics.SetTextValue(metricsDBConnectionStateName, connectedState.String()) }()

	var sql string
	switch lagQuery {
	case _QueryLagPrimary:
		sql = `SELECT
  state,
  sync_state,
  (pg_xlog_location_diff(sent_location, write_location))::FLOAT8 AS durability_lag_bytes,
  (pg_xlog_location_diff(sent_location, flush_location))::FLOAT8 AS flush_lag_bytes,
  (pg_xlog_location_diff(sent_location, replay_location))::FLOAT8 AS visibility_lag_bytes,
  COALESCE(EXTRACT(EPOCH FROM '0'::INTERVAL), 0.0)::FLOAT8 AS visibility_lag_ms
FROM
  pg_catalog.pg_stat_replication
ORDER BY visibility_lag_bytes
LIMIT 1`
	case _QueryLagFollower:
		sql = `SELECT
  'receiving' AS state,
  'applying' AS sync_state,
  0.0::FLOAT8 AS durability_lag_bytes,
  0.0::FLOAT8 AS flush_lag_bytes,
  COALESCE((pg_xlog_location_diff(pg_last_xlog_receive_location(), pg_last_xlog_replay_location()))::FLOAT8, 0.0)::FLOAT8 AS visibility_lag_bytes,
  COALESCE(EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp())::INTERVAL), 0.0)::FLOAT8 AS visibility_lag_ms
LIMIT 1`
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
	var durabilityLagBytes, flushLagBytes, visibilityLagBytes, visibilityLagMs float64 = math.NaN(), math.NaN(), math.NaN(), math.NaN()
	var numRows int
	for rows.Next() {
		err = rows.Scan(&senderState, &syncState, &durabilityLagBytes, &flushLagBytes, &visibilityLagBytes, &visibilityLagMs)
		if err != nil {
			return unknownLag, errors.Wrap(err, "unable to scan lag response")
		}

		numRows++

		// Record useful stats that will be emitted in the logs
		dbStats.senderState.Set(senderState)
		dbStats.peerSyncState.Set(syncState)
		dbStats.durabilityLagBytes.Set(int64(units.Base2Bytes(durabilityLagBytes)))
		dbStats.flushLagBytes.Set(int64(units.Base2Bytes(flushLagBytes)))
		dbStats.visibilityLagBytes.Set(int64(units.Base2Bytes(visibilityLagBytes)))
		dbStats.visibilityLagMs.Set(int64(time.Duration(visibilityLagMs) * (time.Second / time.Millisecond)))

		// Only record values that actually change.  Don't record metrics that are
		// missing on a shard member.
		switch lagQuery {
		case _QueryLagPrimary:
			a.metrics.Gauge(metricsDBLagDurabilityBytes, durabilityLagBytes)
			a.metrics.Gauge(metricsDBLagFlushBytes, flushLagBytes)
		case _QueryLagFollower:
		}

		a.metrics.Gauge(metricsDBLagVisibilityBytes, visibilityLagBytes)
		a.metrics.Gauge(metricsDBLagVisibilityMs, visibilityLagMs)
		a.metrics.SetTextValue(metricsDBPeerSyncState, syncState)
		a.metrics.SetTextValue(metricsDBSenderState, senderState)
	}

	if rows.Err() != nil {
		return unknownLag, errors.Wrap(err, "unable to process lag")
	}

	if numRows == 0 {
		connectedState = _DBConnectionStateDisconnected
		dbStats.connectionState.Set(_DBConnectionStateDisconnected.String())
	} else {
		connectedState = _DBConnectionStateConnected
		dbStats.connectionState.Set(_DBConnectionStateConnected.String())
	}

	return units.Base2Bytes(visibilityLagBytes), nil
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

// predictDBWALFilenames guesses what the filenames are going to be in advance
// of PostgreSQL naievely processing a WAL file.  Use walFile as the seed
// filename to indicate where we are in the WAL stream and forecast N WAL
// filenames where N is the configured level of WAL readahead.  Errors are
// logged, but the return will always include at least one WAL file.
//
// Unlike predictProcWALFilenames(), predictDBWALFilenames() queries the
// database to figure out the current lag and recovery state (versus naively
// assuming the derived LSN from the WAL segment is authoritative).
func (a *Agent) predictDBWALFilenames(walFile pg.WALFilename) ([]pg.WALFilename, error) {
	// If the apply lag of the DB exceeds a threshold, anticipate the correct
	// number of WAL filenames.

	dbState, err := a.dbState()
	if err != nil {
		log.Error().Err(err).Msg("unable to determine if database is primary or not, retrying")
		return []pg.WALFilename{walFile}, err
	}

	switch state := dbState; state {
	case _DBStatePrimary:
		// "Always return at least the current WAL file," ... unless we're the
		// primary.  If we're the primary, there's nothing to fault in so return an
		// empty list.
		return []pg.WALFilename{}, nil
	case _DBStateFollower:
		break
	default:
		panic(fmt.Sprintf("unknown state: %+v", state))
	}

	visibilityLagBytes, err := a.queryLag(_QueryLagFollower)
	if err != nil {
		return nil, errors.Wrap(err, "unable to query follower lag")
	}

	timelineID, lsn, err := pg.ParseWalfile(walFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse WAL file while predicting names from the DB")
	}

	// Clamp the number of bytes we'll readahead in order to prevent reading into
	// the future.
	maxBytes := a.walCache.ReadaheadBytes()
	if maxBytes > visibilityLagBytes {
		maxBytes = visibilityLagBytes
	}

	return lsn.Readahead(timelineID, maxBytes), nil
}

// startDBStats is a periodic stats routine that emits stats regarding the
// database to the log files.
func (a *Agent) startDBStats() {
	for {
		select {
		case <-a.shutdownCtx.Done():
			return
		case <-time.After(config.StatsInterval):
			log.Debug().
				Str(metricsDBSenderState, dbStats.senderState.String()).
				Str(metricsDBState, dbStats.dbState.String()).
				Str(metricsDBPeerSyncState, dbStats.peerSyncState.String()).
				Int64(metricsDBLagDurabilityBytes, dbStats.durabilityLagBytes.Value()).
				Int64(metricsDBLagFlushBytes, dbStats.flushLagBytes.Value()).
				Int64(metricsDBLagVisibilityBytes, dbStats.visibilityLagBytes.Value()).
				Dur(metricsDBLagVisibilityMs, time.Duration(dbStats.visibilityLagMs.Value())).
				Msg("db-stats")
		}
	}
}
