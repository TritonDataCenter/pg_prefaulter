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

package metrics

import (
	"expvar"
)

const (
	DBConnectionStateName = "connected"
	DBLagDurabilityBytes  = "durability-lag"
	DBLagFlushBytes       = "flush-lag"
	DBLagVisibilityBytes  = "visibility-lag"
	DBLagVisibilityMs     = "visibility-lag_ms"
	DBPeerSyncState       = "peer-sync-state"
	DBSenderState         = "sender-state"
	DBState               = "db-state"
	DBTimelineID          = "timeline-id"
	DBVersionPG           = "version-pg"
	DBWALCount            = "num-wal-files"
	VersionSelfCommit     = "version-self-commit"
	VersionSelfDate       = "version-self-date"
	VersionSelfVersion    = "version-self-version"
	WALFileCandidate      = "num-wal-candidates"
)

type _DBStats struct {
	ConnectionState    *expvar.String
	DBState            *expvar.String
	DurabilityLagBytes *expvar.Int
	FlushLagBytes      *expvar.Int
	NumWALFiles        *expvar.Int
	PeerSyncState      *expvar.String
	SenderState        *expvar.String
	TimelineID         *expvar.Int
	VisibilityLagBytes *expvar.Int
	VisibilityLagMs    *expvar.Int
}

var DBStats _DBStats

func init() {
	DBStats = _DBStats{
		ConnectionState:    expvar.NewString(DBConnectionStateName),
		DBState:            expvar.NewString(DBState),
		DurabilityLagBytes: expvar.NewInt(DBLagDurabilityBytes),
		FlushLagBytes:      expvar.NewInt(DBLagFlushBytes),
		NumWALFiles:        expvar.NewInt(DBWALCount),
		PeerSyncState:      expvar.NewString(DBPeerSyncState),
		SenderState:        expvar.NewString(DBSenderState),
		TimelineID:         expvar.NewInt(DBTimelineID),
		VisibilityLagBytes: expvar.NewInt(DBLagVisibilityBytes),
		VisibilityLagMs:    expvar.NewInt(DBLagVisibilityMs),
	}
}
