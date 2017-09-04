package config

import (
	"fmt"
	"strings"

	cgm "github.com/circonus-labs/circonus-gometrics"
	"github.com/jackc/pgx"
	"github.com/joyent/pg_prefaulter/buildtime"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

type (
	DBPool  = pgx.ConnPoolConfig
	Metrics = cgm.Config
)

type Config struct {
	DBPool
	*Metrics
}

func NewDefault() Config {
	cmc := &cgm.Config{}
	if viper.GetBool(KeyCirconusEnabled) {
		cmc.Interval = "10s"
		{
			// FIXME(seanc@): Need a facade that satisfies stdlog.Logger interface, not zerolog/log
			// cgmlog := log.Logger.With().Str("module", "circonus").Logger()
			// cmc.Log = cgmlog
		}
		cmc.Debug = IsDebug()
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

		cmc.CheckManager.Check.SearchTag = viper.GetString(KeyCirconusCheckSearchTag)
		cmc.CheckManager.Check.Secret = viper.GetString(KeyCirconusCheckSecret)
		cmc.CheckManager.Check.Tags = viper.GetString(KeyCirconusCheckTags)
		cmc.CheckManager.Check.MaxURLAge = viper.GetString(KeyCirconusCheckMaxURLAge)
		cmc.CheckManager.Check.ForceMetricActivation = viper.GetString(KeyCirconusCheckForceMetricActivation)

		// Broker configuration options
		cmc.CheckManager.Broker.ID = viper.GetString(KeyCirconusBrokerID)
		cmc.CheckManager.Broker.SelectTag = viper.GetString(KeyCirconusBrokerSelectTag)
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

	return Config{
		DBPool: pgx.ConnPoolConfig{
			AcquireTimeout: 0,
			MaxConnections: 5,

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
	}
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
