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

package config

import (
	"fmt"
	"strings"
	"time"

	cgm "github.com/circonus-labs/circonus-gometrics"
	"github.com/jackc/pgx"
	"github.com/joyent/pg_prefaulter/buildtime"
	"github.com/joyent/pg_prefaulter/pg"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"golang.org/x/sys/unix"
)

type (
	DBPool  = pgx.ConnPoolConfig
	Metrics = cgm.Config
)

type Config struct {
	DBPool
	*Metrics

	FHCacheConfig
	IOCacheConfig
	WALCacheConfig
}

type FHCacheConfig struct {
	MaxOpenFiles uint
	Size         uint
	TTL          time.Duration
	PGDataPath   string
}

type IOCacheConfig struct {
	MaxConcurrentIOs uint
	Size             uint
	TTL              time.Duration
}

type WALMode int

const (
	WALModeDefault WALMode = iota
	WALModePG
	WALModeXLog
)

type WALCacheConfig struct {
	Mode         WALMode
	ReadAhead    uint32
	PGDataPath   string
	XLogDumpPath string
}

func NewDefault() (*Config, error) {
	cmc := &cgm.Config{}
	if viper.GetBool(KeyCirconusEnabled) {
		cmc.Interval = "10s"
		{
			// FIXME(seanc@): Need a facade that satisfies stdlog.Logger interface, not zerolog/log
			// cgmlog := log.Logger.With().Str("module", "circonus").Logger()
			// cmc.Log = cgmlog
		}
		cmc.Debug = viper.GetBool(KeyCirconusDebug)
		cmc.ResetCounters = "false"
		cmc.ResetGauges = "true"
		cmc.ResetHistograms = "true"
		cmc.ResetText = "true"

		cmc.CheckManager.API.TokenKey = viper.GetString(KeyCirconusAPIToken)
		cmc.CheckManager.API.TokenApp = buildtime.PROGNAME
		cmc.CheckManager.API.URL = viper.GetString(KeyCirconusAPIURL)

		// Check configuration options
		cmc.CheckManager.Check.SubmissionURL = viper.GetString(KeyCirconusCheckSubmissionURL)
		cmc.CheckManager.Check.ID = viper.GetString(KeyCirconusCheckID)
		cmc.CheckManager.Check.InstanceID = viper.GetString(KeyCirconusCheckInstanceID)
		cmc.CheckManager.Check.DisplayName = viper.GetString(KeyCirconusCheckDisplayName)
		cmc.CheckManager.Check.TargetHost = viper.GetString(KeyCirconusCheckTargetHost)

		cmc.CheckManager.Check.SearchTag = strings.ToLower(viper.GetString(KeyCirconusCheckSearchTag))
		cmc.CheckManager.Check.Secret = viper.GetString(KeyCirconusCheckSecret)
		cmc.CheckManager.Check.Tags = strings.ToLower(viper.GetString(KeyCirconusCheckTags))
		cmc.CheckManager.Check.MaxURLAge = viper.GetString(KeyCirconusCheckMaxURLAge)
		cmc.CheckManager.Check.ForceMetricActivation = viper.GetString(KeyCirconusCheckForceMetricActivation)

		// Broker configuration options
		cmc.CheckManager.Broker.ID = viper.GetString(KeyCirconusBrokerID)
		cmc.CheckManager.Broker.SelectTag = strings.ToLower(viper.GetString(KeyCirconusBrokerSelectTag))
		cmc.CheckManager.Broker.MaxResponseTime = viper.GetString(KeyCirconusBrokerMaxResponseTime)
	}

	var pgxLogLevel int = pgx.LogLevelInfo
	switch logLevel := strings.ToUpper(viper.GetString(KeyLogLevel)); logLevel {
	case "FATAL":
		pgxLogLevel = pgx.LogLevelNone
	case "ERROR":
		pgxLogLevel = pgx.LogLevelError
	case "WARN":
		pgxLogLevel = pgx.LogLevelWarn
	case "INFO":
		pgxLogLevel = pgx.LogLevelInfo
	case "DEBUG":
		// pgxLogLevel = pgx.LogLevelTrace // NOTE(seanc@): There is no way to
		// enable trace-level logging atm.
		pgxLogLevel = pgx.LogLevelDebug
	default:
		panic(fmt.Sprintf("unsupported log level: %q", logLevel))
	}

	fhConfig := FHCacheConfig{}
	{
		const (
			defaultTTL          = 3600 * time.Second
			numReservedFDs uint = 50
		)

		if fhConfig.MaxOpenFiles == 0 {
			var procNumFiles unix.Rlimit
			if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &procNumFiles); err != nil {
				return nil, errors.Wrap(err, "unable to determine rlimits for number of files")
			}
			fhConfig.MaxOpenFiles = uint(procNumFiles.Cur)
			fhConfig.Size = fhConfig.MaxOpenFiles - numReservedFDs
		} else {
			// TODO(seanc@): Allow a user to set this value manually
		}

		fhConfig.PGDataPath = viper.GetString(KeyPGData)
		// Artificially clamp the size of the filehandle cache.  For some systems
		// this may be running with an elevated rlimit and 65K open files.  That's
		// not necessarily a bad thing, but 2K files could be as much as ~2TB of
		// maxed out PG segments.
		//
		// FIXME(seanc@): Make 2K constant a tunable
		if fhConfig.Size > 2000 {
			fhConfig.Size = 2000
		}

		fhConfig.TTL = defaultTTL
	}

	ioConfig := IOCacheConfig{}
	{
		const (
			driveOpsPerSec = 150
			numVDevs       = 6
			drivesPerVDev  = 2
			headsPerDrive  = 4
			efficiency     = 0.5
			defaultTTL     = 86400 * time.Second

			// TODO(seanc@): Investigation is required to figure out if maxConcurrentIOs
			// should be clamped to the max number o pages in a given WAL file *
			// walReadAhead.  At some point the scheduling of the IOs is going to be more
			// burdensome than actually doing the IOs, but maybe that will only be a
			// visible problem with SSDs. ¯\_(ツ)_/¯
			defaultMaxConcurrentIOs = uint((driveOpsPerSec * numVDevs * drivesPerVDev * headsPerDrive) * efficiency)

			// ioCacheSize is set to cache all operations for ~100 WAL files
			ioCacheSize uint = 100 * uint(pg.WALFileSize/pg.WALPageSize)
		)

		if viper.IsSet(KeyNumIOThreads) || viper.GetInt(KeyNumIOThreads) == 0 {
			ioConfig.MaxConcurrentIOs = defaultMaxConcurrentIOs
		} else {
			ioConfig.MaxConcurrentIOs = uint(viper.GetInt(KeyNumIOThreads))
		}

		ioConfig.Size = ioCacheSize
		ioConfig.TTL = defaultTTL
	}

	walConfig := WALCacheConfig{}
	{
		switch mode := viper.GetString(KeyXLogMode); mode {
		case "xlog":
			walConfig.Mode = WALModeXLog
		case "pg":
			walConfig.Mode = WALModePG
		default:
			panic(fmt.Sprintf("unsupported %q mode: %q", KeyXLogMode, mode))
		}

		walConfig.PGDataPath = viper.GetString(KeyPGData)
		walConfig.ReadAhead = uint32(viper.GetInt(KeyWALReadAhead))
		walConfig.XLogDumpPath = viper.GetString(KeyXLogPath)
	}

	return &Config{
		DBPool: pgx.ConnPoolConfig{
			MaxConnections: 5,
			AfterConnect:   nil,
			AcquireTimeout: 0,

			ConnConfig: pgx.ConnConfig{
				Database: viper.GetString(KeyPGDatabase),
				User:     viper.GetString(KeyPGUser),
				Password: viper.GetString(KeyPGPassword),
				Host:     viper.GetString(KeyPGHost),
				Port:     cast.ToUint16(viper.GetInt(KeyPGPort)),
				// TLSConfig: &tls.Config{}, // TODO(seanc@): need to generate a TLS
				// config

				// FIXME(seanc@): Need to write a zerolog facade that satisfies the pgx logger interface
				// Logger:   log.Logger.With().Str("module", "pgx").Logger(),
				LogLevel: pgxLogLevel,
				RuntimeParams: map[string]string{
					"application_name": buildtime.PROGNAME,
				},
			},
		},
		Metrics: cmc,

		FHCacheConfig:  fhConfig,
		IOCacheConfig:  ioConfig,
		WALCacheConfig: walConfig,
	}, nil
}

// IsDebug returns true when the server is configured for debug level
func IsDebug() bool {
	switch logLevel := strings.ToUpper(viper.GetString(KeyLogLevel)); logLevel {
	case "DEBUG":
		return true
	default:
		return false
	}
}

// ValidStringArg takes a viper key and a list of valid args.  If the key is not
// valid, return an error.
func ValidStringArg(argname string, validArgs []string) error {
	argMap := make(map[string]struct{}, len(validArgs))
	for _, a := range validArgs {
		argMap[a] = struct{}{}
	}

	if _, found := argMap[viper.GetString(argname)]; !found {
		// FIXME(seanc@): map the viper key back to a CLI arg long opt
		return fmt.Errorf("invalid %s (HINT: valid args: %q)", argname, strings.Join(validArgs, ", "))
	}

	return nil
}
