package dbconfig

import "fmt"

// Postgres test database connection parameters
const (
	Host     = "localhost"
	Port     = 5432
	User     = "jet"
	Password = "jet"
	DBName   = "jetdb"
)

// PostgresConnectString is PostgreSQL test database connection string
var PostgresConnectString = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", Host, Port, User, Password, DBName)

// MySQL test database connection parameters
const (
	MySqLHost     = "localhost"
	MySQLPort     = 3306
	MySQLUser     = "jet"
	MySQLPassword = "jet"
)

// MySQLConnectionString is MySQL driver connection string to test database
var MySQLConnectionString = fmt.Sprintf("%s:%s@tcp(%s:%d)/", MySQLUser, MySQLPassword, MySqLHost, MySQLPort)
