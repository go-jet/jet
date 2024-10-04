package mysql

import (
	"fmt"
)

var (
	MySqLHost   = "127.0.0.1"
	MySQLPort   = 50902
	MariaDBHost = "127.0.0.1"
	MariaDBPort = 50903
)

// MySQL test database connection parameters
const (
	MySqlSourceEnvKey = "MY_SQL_SOURCE"
	MariaDB           = "MariaDB"

	MySQLUser     = "jet"
	MySQLPassword = "jet"

	MariaDBUser     = "jet"
	MariaDBPassword = "jet"
)

// ConnectionString is MySQL connection string for test database
func ConnectionString(isMariaDB bool, dbName string) string {
	if isMariaDB {
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", MariaDBUser, MariaDBPassword, MariaDBHost, MariaDBPort, dbName)
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", MySQLUser, MySQLPassword, MySqLHost, MySQLPort, dbName)
}
