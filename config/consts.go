package config

const (
	KeyCirconusAPIToken                   = "circonus.api.token"
	KeyCirconusAPIURL                     = "circonus.api.url"
	KeyCirconusBrokerID                   = "circonus.broker.id"
	KeyCirconusBrokerMaxResponseTime      = "circonus.broker.max_response_time"
	KeyCirconusBrokerSelectTag            = "circonus.broker.select_tag"
	KeyCirconusCheckDisplayName           = "circonus.check.display_name"
	KeyCirconusCheckForceMetricActivation = "circonus.check.force_metric_activation"
	KeyCirconusCheckID                    = "circonus.check.id"
	KeyCirconusCheckInstanceID            = "circonus.check.instance_id"
	KeyCirconusCheckMaxURLAge             = "circonus.check.max_url_age"
	KeyCirconusCheckSearchTag             = "circonus.check.search_tag"
	KeyCirconusCheckSecret                = "circonus.check.secret"
	KeyCirconusCheckSubmissionURL         = "circonus.check.submission_url"
	KeyCirconusCheckTags                  = "circonus.check.tags"
	KeyCirconusCheckTargetHost            = "circonus.check.target_host"
	KeyCirconusDebug                      = "circonus.debug"
	KeyCirconusEnabled                    = "circonus.enabled"

	KeyGoogleAgentEnable = "google.agent.enabled"

	KeyLogLevel = "log.level"

	KeyPGData         = "postgresql.pgdata"
	KeyPGDatabase     = "postgresql.database"
	KeyPGHost         = "postgresql.host"
	KeyPGMode         = "postgresql.mode"
	KeyPGPassword     = "postgresql.password"
	KeyPGPollInterval = "postgresql.poll-interval"
	KeyPGPort         = "postgresql.port"
	KeyPGUser         = "postgresql.user"

	KeyWALReadAhead = "postgresql.wal.read-ahead"
	KeyWALThreads   = "postgresql.wal.threads"

	KeyXLogMode = "postgresql.xlog.mode"
	KeyXLogPath = "postgresql.xlog.pg_xlogdump-path"
)
