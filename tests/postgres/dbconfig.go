package postgres

import "fmt"

var (
	PgHost        = DefaultPgHost
	PgPort        = DefaultPgPort
	CockroachHost = DefaultCockroachHost
	CockroachPort = DefaultCockroachPort
)

// Postgres test database connection parameters
const (
	PgSourceEnvKey = "PG_SOURCE"
	CockroachDB    = "COCKROACH_DB"
	DefaultPgHost  = "localhost"
	DefaultPgPort  = 50901
	PgUser         = "jet"
	PgPassword     = "jet"
	PgDBName       = "jetdb"
)

// Postgres test database connection parameters
const (
	DefaultCockroachHost = "localhost"
	DefaultCockroachPort = 26257
	CockroachUser        = "jet"
	CockroachPassword    = "jet"
	CockroachDBName      = "jetdb"
)

func PgConnectionString(host string, port int, user, password, dbName string) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbName)
}
