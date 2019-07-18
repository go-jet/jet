package dbconfig

import "fmt"

// test database connection parameters
const (
	Host     = "localhost"
	Port     = 5432
	User     = "jet"
	Password = "jet"
	DBName   = "jetdb"
)

// ConnectString is PostgreSQL connection string
var ConnectString = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", Host, Port, User, Password, DBName)
