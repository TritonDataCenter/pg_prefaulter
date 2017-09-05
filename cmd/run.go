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

package cmd

import (
	"fmt"
	"os"

	"github.com/joyent/pg_prefaulter/agent"
	"github.com/joyent/pg_prefaulter/buildtime"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Process-wide stats, all managed as atomic integers
var (
	walBytesRead  uint64 // Number of bytes pread(2)
	walReadErrors uint64 // Number of pread(2) errors
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: fmt.Sprintf("Run %s", buildtime.PROGNAME),
	Long:  fmt.Sprintf(`Run %s and begin faulting in PostgreSQL pages`, buildtime.PROGNAME),

	PreRunE: func(cmd *cobra.Command, args []string) error {
		log.Debug().Str("config-file", viper.ConfigFileUsed()).Msg("")
		log.Debug().Msgf("args: %v", args)

		// Perform input validation
		{
			validArgs := []string{"auto", "primary", "follower"}
			if err := config.ValidStringArg(config.KeyPGMode, validArgs); err != nil {
				return errors.Wrapf(err, "%q validation", config.KeyPGMode)
			}
		}

		{
			validArgs := []string{"pg", "xlog"}
			if err := config.ValidStringArg(config.KeyXLogMode, validArgs); err != nil {
				return errors.Wrapf(err, "%q validation", config.KeyXLogMode)
			}
		}

		{
			_, err := os.Stat(viper.GetString(config.KeyXLogPath))
			if err != nil {
				return errors.Wrapf(err, "failed to stat %s (%q)", config.KeyXLogPath, viper.GetString(config.KeyXLogPath))
			}
		}

		defer func() {
			// FIXME(seanc@): Iterate over known viper keys and automatically log
			// values.
			log.Debug().
				Str(config.KeyPGData, viper.GetString(config.KeyPGData)).
				Str(config.KeyPGHost, viper.GetString(config.KeyPGHost)).
				Uint(config.KeyPGPort, uint(viper.GetInt(config.KeyPGPort))).
				Str(config.KeyPGUser, viper.GetString(config.KeyPGUser)).
				Str(config.KeyXLogMode, viper.GetString(config.KeyXLogMode)).
				Str(config.KeyXLogPath, viper.GetString(config.KeyXLogPath)).
				Dur(config.KeyPGPollInterval, viper.GetDuration(config.KeyPGPollInterval)).
				Uint(config.KeyWALThreads, uint(viper.GetInt(config.KeyWALThreads))).
				Msg("flags")
		}()

		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		log.Info().Msg("Starting " + buildtime.PROGNAME)
		defer log.Info().Msg("Stopped " + buildtime.PROGNAME)

		a, err := agent.New(config.NewDefault())
		if err != nil {
			return errors.Wrap(err, "unable to start agent")
		}
		go a.Start()
		defer a.Stop()

		return a.Wait()
	},
}

func init() {
	RootCmd.AddCommand(runCmd)

	{
		const (
			modeLong        = "mode"
			modeShort       = "m"
			modeAutoDefault = "auto"
		)
		// FIXME(seanc@): the list of available options needs to be pulled from a
		// global constant.  This information is duplicated elsewhere in the
		// validation.
		runCmd.Flags().StringP(modeLong, modeShort, modeAutoDefault,
			`Mode of operation of the database: "auto", "primary", "follower"`)
		viper.BindPFlag(config.KeyPGMode, runCmd.Flags().Lookup(modeLong))
		viper.SetDefault(config.KeyPGMode, modeAutoDefault)
	}

	{
		const (
			pollIntervalLong    = "poll-interval"
			defaultPollInterval = "1s"
		)

		runCmd.Flags().StringP(pollIntervalLong, "i", defaultPollInterval, "Interval to poll the database for state change")
		viper.BindPFlag(config.KeyPGPollInterval, runCmd.Flags().Lookup(pollIntervalLong))
		viper.SetDefault(config.KeyPGPollInterval, defaultPollInterval)
	}

	{
		const (
			walReadAheadLong    = "wal-readahead"
			walReadAheadShort   = "n"
			walReadAheadDefault = 4
		)

		runCmd.Flags().UintP(walReadAheadLong, walReadAheadShort,
			walReadAheadDefault, "Number of WAL entries to perform read-ahead into")
		viper.BindPFlag(config.KeyWALReadAhead, runCmd.Flags().Lookup(walReadAheadLong))
		viper.SetDefault(config.KeyWALReadAhead, walReadAheadDefault)
	}

	{
		const (
			walThreadsLong    = "wal-threads"
			walThreadsShort   = "t"
			walThreadsDefault = 4
		)

		runCmd.Flags().UintP(walThreadsLong, walThreadsShort,
			walThreadsDefault, "Number of conurrent prefetch threads per WAL file")
		viper.BindPFlag(config.KeyWALThreads, runCmd.Flags().Lookup(walThreadsLong))
		viper.SetDefault(config.KeyWALThreads, walThreadsDefault)
	}

	{
		const (
			pgXLogdumpPathLong  = "xlogdump-bin"
			pgXLogdumpPathShort = "x"
			// TODO(seanc@): This could/should probably be a build-time constant that
			// is platform specific.  Similarly, there should probably be a path that
			// is independent of the binary name.
			pgXLogdumpPathDefault = "/usr/local/bin/pg_xlogdump"
		)

		runCmd.Flags().StringP(pgXLogdumpPathLong, pgXLogdumpPathShort,
			pgXLogdumpPathDefault, "Path to pg_xlogdump(1)")
		viper.BindPFlag(config.KeyXLogPath, runCmd.Flags().Lookup(pgXLogdumpPathLong))
		viper.SetDefault(config.KeyXLogPath, pgXLogdumpPathDefault)
	}

	{
		const (
			pgXLogdumpModeLong    = "xlog-mode"
			pgXLogdumpModeShort   = "X"
			pgXLogdumpModeDefault = "pg"
		)
		runCmd.Flags().StringP(pgXLogdumpModeLong, pgXLogdumpModeShort,
			pgXLogdumpModeDefault, `pg_xlogdump(1) variant: "xlog" or "pg"`)
		viper.BindPFlag(config.KeyXLogMode, runCmd.Flags().Lookup(pgXLogdumpModeLong))
		viper.SetDefault(config.KeyXLogMode, pgXLogdumpModeDefault)
	}
}
