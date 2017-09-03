package config

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx"
	"github.com/joyent/pg_prefaulter/buildtime"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

type Config struct {
	pgx.ConnPoolConfig
}

func NewDefault() Config {
	return Config{
		pgx.ConnPoolConfig{
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

				// TODO(seanc@): Create a logger interface that wraps rs/zerolog
				// Logger: logger,
				// LogLevel: int,
				RuntimeParams: map[string]string{
					"application_name": buildtime.PROGNAME,
				},
			},
		},
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
