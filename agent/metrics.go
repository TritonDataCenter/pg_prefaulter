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
	"expvar"
)

const (
	metricsDBConnectionStateName = "connected"
	metricsDBLagDurabilityBytes  = "durability-lag"
	metricsDBLagFlushBytes       = "flush-lag"
	metricsDBLagVisibilityBytes  = "visibility-lag"
	metricsDBLagVisibilityMs     = "visibility-lag_ms"
	metricsDBPeerSyncState       = "peer-sync-state"
	metricsDBSenderState         = "sender-state"
	metricsDBState               = "db-state"
	metricsDBTimelineID          = "timeline-id"
	metricsDBVersionPG           = "version-pg"
	metricsDBWALCount            = "num-wal-files"
	metricsVersionSelfCommit     = "version-self-commit"
	metricsVersionSelfDate       = "version-self-date"
	metricsVersionSelfVersion    = "version-self-version"
	metricsWALFileCandidate      = "num-wal-candidates"
)

type _DBStats struct {
	connectionState    *expvar.String
	numWALFiles        *expvar.Int
	dbState            *expvar.String
	timelineID         *expvar.Int
	durabilityLagBytes *expvar.Int
	flushLagBytes      *expvar.Int
	senderState        *expvar.String
	peerSyncState      *expvar.String
	visibilityLagBytes *expvar.Int
	visibilityLagMs    *expvar.Int
}

var dbStats _DBStats

func init() {
	dbStats = _DBStats{
		connectionState:    expvar.NewString(metricsDBConnectionStateName),
		numWALFiles:        expvar.NewInt(metricsDBWALCount),
		dbState:            expvar.NewString(metricsDBState),
		timelineID:         expvar.NewInt(metricsDBTimelineID),
		durabilityLagBytes: expvar.NewInt(metricsDBLagDurabilityBytes),
		flushLagBytes:      expvar.NewInt(metricsDBLagFlushBytes),
		senderState:        expvar.NewString(metricsDBSenderState),
		peerSyncState:      expvar.NewString(metricsDBPeerSyncState),
		visibilityLagBytes: expvar.NewInt(metricsDBLagVisibilityBytes),
		visibilityLagMs:    expvar.NewInt(metricsDBLagVisibilityMs),
	}
}
