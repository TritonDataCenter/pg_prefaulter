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
				Msg("flags")
		}()

		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		log.Info().Int("pid", os.Getpid()).Msg("Starting " + buildtime.PROGNAME)
		defer log.Info().Int("pid", os.Getpid()).Msg("Stopped " + buildtime.PROGNAME)

		cfg, err := config.NewDefault()
		if err != nil {
			return errors.Wrap(err, "unable to generate default config")
		}

		a, err := agent.New(cfg)
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
			key          = config.KeyPGMode
			longName     = "mode"
			shortName    = "m"
			defaultValue = "auto"
			description  = `Mode of operation of the database: "auto", "primary", "follower"`
		)
		// FIXME(seanc@): the list of available options needs to be pulled from a
		// global constant.  This information is duplicated elsewhere in the
		// validation.
		runCmd.Flags().StringP(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, runCmd.Flags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			key          = config.KeyPGPollInterval
			longName     = "poll-interval"
			shortName    = "i"
			defaultValue = "1s"
			description  = "Interval to poll the database for state change"
		)

		runCmd.Flags().StringP(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, runCmd.Flags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			key          = config.KeyRetryDBInit
			longName     = "retry-db-init"
			defaultValue = false
			description  = `Retry connecting to the database during initialization`
		)
		runCmd.Flags().Bool(longName, defaultValue, description)
		viper.BindPFlag(key, runCmd.Flags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			key          = config.KeyWALReadAhead
			longName     = "wal-readahead"
			shortName    = "n"
			defaultValue = 4
			description  = "Number of WAL entries to perform read-ahead into"
		)

		runCmd.Flags().UintP(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, runCmd.Flags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			key          = config.KeyNumIOThreads
			longName     = "num-io-threads"
			shortName    = "N"
			defaultValue = 0
			description  = "Number of IO threads to spawn for IOs"
		)

		runCmd.Flags().UintP(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, runCmd.Flags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			key       = config.KeyXLogPath
			longName  = "xlogdump-bin"
			shortName = "x"
			// TODO(seanc@): This could/should probably be a build-time constant that
			// is platform specific.  Similarly, there should probably be a path that
			// is independent of the binary name.
			defaultValue = "/usr/local/bin/pg_xlogdump"
			description  = "Path to pg_xlogdump(1)"
		)

		runCmd.Flags().StringP(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, runCmd.Flags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			key          = config.KeyXLogMode
			longName     = "xlog-mode"
			shortName    = "X"
			defaultValue = "pg"
			description  = `pg_xlogdump(1) variant: "xlog" or "pg"`
		)
		runCmd.Flags().StringP(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, runCmd.Flags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}
}
