package dbconfig

import (
	"fmt"
	"github.com/go-jet/jet/v2/tests/internal/utils/repo"
)

// Postgres test database connection parameters
const (
	PgHost     = "localhost"
	PgPort     = 50901
	PgUser     = "jet"
	PgPassword = "jet"
	PgDBName   = "jetdb"
)

// PostgresConnectString is PostgreSQL test database connection string
var PostgresConnectString = pgConnectionString(PgHost, PgPort, PgUser, PgPassword, PgDBName)

// Postgres test database connection parameters
const (
	CockroachHost     = "localhost"
	CockroachPort     = 26257
	CockroachUser     = "jet"
	CockroachPassword = "jet"
	CockroachDBName   = "jetdb"
)

// CockroachConnectString is Cockroach test database connection string
var CockroachConnectString = pgConnectionString(CockroachHost, CockroachPort, CockroachUser, CockroachPassword, CockroachDBName)

func pgConnectionString(host string, port int, user, password, dbName string) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbName)
}

// MySQL test database connection parameters
const (
	MySqLHost     = "127.0.0.1"
	MySQLPort     = 50902
	MySQLUser     = "jet"
	MySQLPassword = "jet"

	MariaDBHost     = "127.0.0.1"
	MariaDBPort     = 50903
	MariaDBUser     = "jet"
	MariaDBPassword = "jet"
)

// MySQLConnectionString is MySQL connection string for test database
func MySQLConnectionString(isMariaDB bool, dbName string) string {
	if isMariaDB {
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", MariaDBUser, MariaDBPassword, MariaDBHost, MariaDBPort, dbName)
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", MySQLUser, MySQLPassword, MySqLHost, MySQLPort, dbName)
}

// sqllite
var (
	SakilaDBPath     = repo.GetTestDataFilePath("/init/sqlite/sakila.db")
	ChinookDBPath    = repo.GetTestDataFilePath("/init/sqlite/chinook.db")
	TestSampleDBPath = repo.GetTestDataFilePath("/init/sqlite/test_sample.db")
)
