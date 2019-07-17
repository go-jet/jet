package dbconfig

import "fmt"

const (
	Host     = "localhost"
	Port     = 5432
	User     = "jet"
	Password = "jet"
	DBName   = "jetdb"
)

var ConnectString = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", Host, Port, User, Password, DBName)
