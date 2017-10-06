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

import "expvar"

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

var (
	statsDBConnectionState  *expvar.String
	statsDBNumWALFiles      *expvar.Int
	statsDBState            *expvar.String
	statsDBTimelineID       *expvar.Int
	statsDurabilityLagBytes *expvar.Int
	statsFlushLagBytes      *expvar.Int
	statsSenderState        *expvar.String
	statsSyncState          *expvar.String
	statsVisibilityLagBytes *expvar.Int
	statsVisibilityLagMs    *expvar.Int
)

func init() {
	statsDBConnectionState = expvar.NewString(metricsDBConnectionStateName)
	statsDBNumWALFiles = expvar.NewInt(metricsDBWALCount)
	statsDBState = expvar.NewString(metricsDBState)
	statsDBTimelineID = expvar.NewInt(metricsDBTimelineID)
	statsDurabilityLagBytes = expvar.NewInt(metricsDBLagDurabilityBytes)
	statsFlushLagBytes = expvar.NewInt(metricsDBLagFlushBytes)
	statsSenderState = expvar.NewString(metricsDBSenderState)
	statsSyncState = expvar.NewString(metricsDBPeerSyncState)
	statsVisibilityLagBytes = expvar.NewInt(metricsDBLagVisibilityBytes)
	statsVisibilityLagMs = expvar.NewInt(metricsDBLagVisibilityMs)
}
