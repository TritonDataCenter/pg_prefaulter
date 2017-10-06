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

package lib

import (
	"context"
	"time"

	"github.com/bluele/gcache"
	"github.com/joyent/pg_prefaulter/config"
	log "github.com/rs/zerolog/log"
)

// IsShuttingDown is a convenience helper that returns true when the context is
// Done.  True indicates an orderly shutdown is to begin immediately.
func IsShuttingDown(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// LogCacheStats emits logs periodically with cache hit, miss, lookup count, and
// hit rates.
func LogCacheStats(ctx context.Context, c gcache.Cache, cacheName string) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(config.StatsInterval):
			log.Debug().
				Uint64("hit", c.HitCount()).
				Uint64("miss", c.MissCount()).
				Uint64("lookup", c.LookupCount()).
				Float64("hit-rate", c.HitRate()).
				Msg(cacheName)
		}
	}
}
