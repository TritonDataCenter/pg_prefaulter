package buildtime

const (
	PROGNAME = "pg_prefaulter"
)

// These variables are set at build time
var (
	COMMIT  = "unknown"
	DATE    = "unknown"
	TAG     = "unknown"
	VERSION = "dev"
)
