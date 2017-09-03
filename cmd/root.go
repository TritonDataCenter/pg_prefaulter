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
	Use:   "pg_walfaulter",
	Short: "pg_walfaulter pre-faults PostgreSQL WAL pages",
	Long: `PostgreSQL's WAL-receiver applies WAL files in serial and relies on the
operating system's filesystem cache.  pg_walfaulter decodes WAL pages and
attempts to pre-fault the page into the OS'es cache in advance of the the WAL
receiver needing the page.
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
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.pg_walfaulter.yaml)")

	{
		const (
			pgdataPathLong    = "pgdata"
			pgdataPathShort   = "D"
			pgdataPathDefault = "pgdata"
		)

		RootCmd.Flags().StringP(pgdataPathLong, pgdataPathShort, pgdataPathDefault, "Path to PGDATA")
		viper.BindPFlag(config.KeyPGData, runCmd.Flags().Lookup(pgdataPathLong))
		viper.BindEnv(config.KeyPGData, "PGDATA")
	}

	{
		const (
			defaultPGHostname = "/tmp"
			pgHostLong        = "hostname"
			pgHostShort       = "H"
		)

		RootCmd.Flags().StringP(pgHostLong, pgHostShort, defaultPGHostname, "Hostname to connect to PostgreSQL")
		viper.BindPFlag(config.KeyPGHost, runCmd.Flags().Lookup(pgHostLong))
		viper.BindEnv(config.KeyPGHost, "PGHOST")
	}

	{
		const (
			defaultPGPort = 5432
			pgPortLong    = "port"
			pgPortShort   = "p"
		)

		RootCmd.Flags().UintP(pgPortLong, pgPortShort, defaultPGPort, "Hostname to connect to PostgreSQL")
		viper.BindPFlag(config.KeyPGPort, runCmd.Flags().Lookup(pgPortLong))
		viper.BindEnv(config.KeyPGPort, "PGPORT")
	}

	{
		const (
			pgdatabaseLong    = "database"
			pgdatabaseShort   = "d"
			pgdatabaseDefault = "postgres"
		)

		RootCmd.Flags().StringP(pgdatabaseLong, pgdatabaseShort, pgdatabaseDefault, "Database name to connect to")
		viper.BindPFlag(config.KeyPGDatabase, runCmd.Flags().Lookup(pgdatabaseLong))
		viper.BindEnv(config.KeyPGDatabase, "PGDATABASE")
	}

	{
		const (
			defaultPGUsername = "postgres"
			pgUsernameLong    = "username"
			pgUsernameShort   = "U"
		)

		RootCmd.Flags().StringP(pgUsernameLong, pgUsernameShort, defaultPGUsername, "Username to connect to PostgreSQL")
		viper.BindPFlag(config.KeyPGUser, runCmd.Flags().Lookup(pgUsernameLong))
		viper.BindEnv(config.KeyPGUser, "PGUSER")
	}

	{
		viper.BindEnv(config.KeyPGPassword, "PGPASSWORD")
	}

	{
		const (
			disableAgentLong  = "disable-agent"
			disableAgentShort = "A"
		)

		RootCmd.Flags().BoolP(disableAgentLong, disableAgentShort, false, "Disable the gops(1) agent interface")
		viper.BindPFlag(config.KeyDisableAgent, runCmd.Flags().Lookup(disableAgentLong))
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
