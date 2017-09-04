package agent

import (
	log "github.com/rs/zerolog/log"
)

// runPrimary is executed when talking to a writable database.
func (a *Agent) runPrimary() (loopImmediately bool) {
	log.Debug().Msg(_DBStatePrimary.String())
	// Connect to the primary and see what the lag is in bytes between the primary
	// and its connected followers.  Report out a histogram of lag.

	_, err := a.queryLag(_QueryLagPrimary)
	if err != nil {
		log.Error().Err(err).Msg("unable to query primary lag")
		return false
	}

	err = a.queryLastLog()
	if err != nil {
		log.Error().Err(err).Msg("unable to query last WAL lag")
		return false
	}

	return false
}
