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
	KeyGoogleAgentEnable                  = "google.agent.enabled"

	KeyLogLevel = "log.level"

	KeyAgentJSONLogging = "run.json-logs"
	KeyNumIOThreads     = "run.num-io-threads"
	KeyRetryDBInit      = "run.retry-db-init"
	KeyAgentUseColor    = "run.use-color"

	KeyPGData         = "postgresql.pgdata"
	KeyPGDatabase     = "postgresql.database"
	KeyPGHost         = "postgresql.host"
	KeyPGMode         = "postgresql.mode"
	KeyPGPassword     = "postgresql.password"
	KeyPGPollInterval = "postgresql.poll-interval"
	KeyPGPort         = "postgresql.port"
	KeyPGUser         = "postgresql.user"

	KeyWALReadahead = "postgresql.wal.readahead-bytes"
	KeyWALThreads   = "postgresql.wal.threads"

	KeyXLogMode = "postgresql.xlog.mode"
	KeyXLogPath = "postgresql.xlog.pg_xlogdump-path"
)

const (
	MetricsSysPreadLatency = "ioc-sys-pread-ms"
	MetricsPrefaultCount   = "ioc-prefault-count"
	MetricsIOCacheHit      = "ioc-hit"
	MetricsIOCacheMiss     = "ioc-miss"

	MetricsSysCloseCount      = "fh-sys-close-count"
	MetricsSysOpenCount       = "fh-sys-open-count"
	MetricsSysOpenLatency     = "fh-sys-open-us"
	MetricsSysPreadBytes      = "fh-sys-pread-bytes"
	MetricsXLogDumpErrorCount = "fh-xlogdump-error-count"

	MetricsWALFaultCount         = "wal-file-fault-count"
	MetricsWALFaultTime          = "wal-file-fault-time"
	MetricsXLogDumpLen           = "wal-xlogdump-out-len"
	MetricsXLogDumpBlocksMatched = "wal-xlogdump-blocks-matched"
	MetricsXLogDumpLinesMatched  = "wal-xlogdump-lines-matched"
	MetricsXLogDumpLinesScanned  = "wal-xlogdump-lines-scanned"
	MetricsXLogPrefaulted        = "wal-xlog-prefaulted-count"
)

const (
	// Use a log format that resembles time.RFC3339Nano but includes all trailing
	// zeros so that we get fixed-width logging.
	LogFormat = "2006-01-02T15:04:05.000000000Z07:00"
)
