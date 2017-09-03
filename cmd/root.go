// Copyright © 2017 Sean Chittenden <sean@chittenden.org>
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
	_ "expvar"
	"fmt"
	stdlog "log"
	"os"
	"strings"
	"time"

	"github.com/google/gops/agent"
	"github.com/joyent/pg_prefaulter/config"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// CLI flags
var (
	cfgFile  string
	logLevel string

	gopsAgentEndpoint string = "localhost:5431"
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "pg_prefaulter",
	Short: "pg_prefaulter pre-faults PostgreSQL heap pages based on WAL files",
	Long: `
PostgreSQL's WAL-receiver applies WAL files in serial.  This design implicitly
assumes that the heap page required to apply the WAL entry is within the
operating system's filesystem cache.  If the filesystem cache does not contain
the necessary heap page, the PostgreSQL WAL apply process will be block while
the OS faults in the page from its storage.  For large working sets of data or
when the filesystem cache is cold, this is problematic for streaming replicas
because they will lag and fall behind.

pg_prefaulter(1) mitigates this serially scheduled IO problem by reading WAL
entries via pg_xlogdump(1) and performing parallel pread(2) calls in order to
"pre-fault" the page into the OS's filesystem cache so that when the PostgreSQL
WAL receiver goes to apply a WAL entry to its heap, the page is already loaded
into the OS'es filesystem cache.
`,

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		switch strings.ToUpper(logLevel) {
		case "DEBUG":
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		case "INFO":
			zerolog.SetGlobalLevel(zerolog.InfoLevel)
		case "WARN":
			zerolog.SetGlobalLevel(zerolog.WarnLevel)
		case "ERROR":
			zerolog.SetGlobalLevel(zerolog.ErrorLevel)
		case "FATAL":
			zerolog.SetGlobalLevel(zerolog.FatalLevel)
		default:
			return fmt.Errorf("unsupported error level: %q (supported levels: %s)", logLevel,
				strings.Join([]string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}, " "))
		}

		go func() {
			if viper.GetBool(config.KeyDisableAgent) {
				log.Debug().Msg("gops(1) agent disabled by request")
				return
			}

			options := &agent.Options{
				Addr:              gopsAgentEndpoint,
				NoShutdownCleanup: true,
			}
			log.Debug().Str("agent endpoint", options.Addr).Msg("starting gops(1) agent")
			if err := agent.Listen(options); err != nil {
				log.Fatal().Err(err).Msg("unable to start the gops(1) agent thread")
			}
		}()

		return nil
	},
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// zerolog.TimestampFieldName = "t"
	// zerolog.LevelFieldName = "l"
	// zerolog.MessageFieldName = "m"
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	// os.Stderr isn't guaranteed to be thread-safe, wrap in a sync writer.  Files
	// are guaranteed to be safe, terminals are not.
	zlog := zerolog.New(zerolog.SyncWriter(os.Stderr)).With().Timestamp().Logger()
	log.Logger = zlog

	stdlog.SetFlags(0)
	stdlog.SetOutput(zlog)

	RootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "INFO", "Log level")
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.pg_prefaulter.yaml)")

	{
		const (
			pgdataPathLong    = "pgdata"
			pgdataPathShort   = "D"
			pgdataPathDefault = "pgdata"
		)

		RootCmd.PersistentFlags().StringP(pgdataPathLong, pgdataPathShort, pgdataPathDefault, "Path to PGDATA")
		viper.BindPFlag(config.KeyPGData, RootCmd.PersistentFlags().Lookup(pgdataPathLong))
		viper.BindEnv(config.KeyPGData, "PGDATA")
		viper.SetDefault(config.KeyPGData, pgdataPathDefault)
	}

	{
		const (
			defaultPGHostname = "/tmp"
			pgHostLong        = "hostname"
			pgHostShort       = "H"
		)

		RootCmd.PersistentFlags().StringP(pgHostLong, pgHostShort, defaultPGHostname, "Hostname to connect to PostgreSQL")
		viper.BindPFlag(config.KeyPGHost, RootCmd.PersistentFlags().Lookup(pgHostLong))
		viper.BindEnv(config.KeyPGHost, "PGHOST")
		viper.SetDefault(config.KeyPGHost, config.KeyPGHost)
	}

	{
		const (
			defaultPGPort = 5432
			pgPortLong    = "port"
			pgPortShort   = "p"
		)

		RootCmd.PersistentFlags().UintP(pgPortLong, pgPortShort, defaultPGPort, "Hostname to connect to PostgreSQL")
		viper.BindPFlag(config.KeyPGPort, RootCmd.PersistentFlags().Lookup(pgPortLong))
		viper.BindEnv(config.KeyPGPort, "PGPORT")
		viper.SetDefault(config.KeyPGPort, defaultPGPort)
	}

	{
		const (
			pgdatabaseLong    = "database"
			pgdatabaseShort   = "d"
			pgdatabaseDefault = "postgres"
		)

		RootCmd.PersistentFlags().StringP(pgdatabaseLong, pgdatabaseShort, pgdatabaseDefault, "Database name to connect to")
		viper.BindPFlag(config.KeyPGDatabase, RootCmd.PersistentFlags().Lookup(pgdatabaseLong))
		viper.BindEnv(config.KeyPGDatabase, "PGDATABASE")
		viper.SetDefault(config.KeyPGDatabase, pgdatabaseDefault)
	}

	{
		const (
			defaultPGUsername = "postgres"
			pgUsernameLong    = "username"
			pgUsernameShort   = "U"
		)

		RootCmd.PersistentFlags().StringP(pgUsernameLong, pgUsernameShort, defaultPGUsername, "Username to connect to PostgreSQL")
		viper.BindPFlag(config.KeyPGUser, RootCmd.PersistentFlags().Lookup(pgUsernameLong))
		viper.BindEnv(config.KeyPGUser, "PGUSER")
		viper.SetDefault(config.KeyPGUser, defaultPGUsername)
	}

	{
		const defaultPGPassword = ""
		viper.BindEnv(config.KeyPGPassword, "PGPASSWORD")
		viper.SetDefault(config.KeyPGPassword, defaultPGPassword)
	}

	{
		const (
			disableAgentLong    = "disable-agent"
			disableAgentShort   = "A"
			defaultDisableAgent = false
		)

		RootCmd.PersistentFlags().BoolP(disableAgentLong, disableAgentShort, defaultDisableAgent, "Disable the gops(1) agent interface")
		viper.BindPFlag(config.KeyDisableAgent, RootCmd.PersistentFlags().Lookup(disableAgentLong))
		viper.SetDefault(config.KeyDisableAgent, defaultDisableAgent)
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(home)
			os.Exit(1)
		}

		// Search config in home directory with name ".cobra" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".pg_prefaulter")
	}

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Debug().Msgf("Using config file: %s", viper.ConfigFileUsed())
	}
}
