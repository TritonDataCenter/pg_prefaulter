package config

import (
	"github.com/jackc/pgx"
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
				Database: "postgres",
				User:     viper.GetString(KeyPGUser),
				Password: viper.GetString(KeyPGPassword),
				Host:     viper.GetString(KeyPGHost),
				Port:     5432,

				// TODO(seanc@): Create a logger interface that wraps rs/zerolog
				// Logger: logger,
				// LogLevel: int,
				RuntimeParams: map[string]string{
					"application_name": "pg_prefaulter",
				},
			},
		},
	}
}
