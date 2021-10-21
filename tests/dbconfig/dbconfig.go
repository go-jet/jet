package dbconfig

import (
	"fmt"
	"github.com/go-jet/jet/v2/tests/internal/utils/repo"
)

// Postgres test database connection parameters
const (
	PgHost     = "localhost"
	PgPort     = 5432
	PgUser     = "jet"
	PgPassword = "jet"
	PgDBName   = "jetdb"
)

// PostgresConnectString is PostgreSQL test database connection string
var PostgresConnectString = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", PgHost, PgPort, PgUser, PgPassword, PgDBName)

// MySQL test database connection parameters
const (
	MySqLHost     = "localhost"
	MySQLPort     = 3306
	MySQLUser     = "jet"
	MySQLPassword = "jet"
)

// MySQLConnectionString is MySQL driver connection string to test database
var MySQLConnectionString = fmt.Sprintf("%s:%s@tcp(%s:%d)/", MySQLUser, MySQLPassword, MySqLHost, MySQLPort)

// sqllite
var (
	SakilaDBPath     = repo.GetTestDataFilePath("/init/sqlite/sakila.db")
	ChinookDBPath    = repo.GetTestDataFilePath("/init/sqlite/chinook.db")
	TestSampleDBPath = repo.GetTestDataFilePath("/init/sqlite/test_sample.db")
)
