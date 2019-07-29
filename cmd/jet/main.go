package main

import (
	"flag"
	"fmt"
	"github.com/go-jet/jet"
	"github.com/go-jet/jet/generator/mysql"
	"github.com/go-jet/jet/generator/postgres"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"os"
)

var (
	source string

	host       string
	port       int
	user       string
	password   string
	sslmode    string
	params     string
	dbName     string
	schemaName string

	destDir string
)

func init() {
	flag.StringVar(&source, "source", string(jet.PostgreSQL.Name), "Database name")

	flag.StringVar(&host, "host", "", "Database host path (Example: localhost)")
	flag.IntVar(&port, "port", 0, "Database port")
	flag.StringVar(&user, "user", "", "Database user")
	flag.StringVar(&password, "password", "", "The user’s password")
	flag.StringVar(&sslmode, "sslmode", "disable", "Whether or not to use SSL(optional)")
	flag.StringVar(&params, "params", "", "Additional connection string parameters(optional)")
	flag.StringVar(&dbName, "dbname", "", "name of the database")
	flag.StringVar(&schemaName, "schema", "public", "Database schema name.")

	flag.StringVar(&destDir, "path", "", "Destination dir for files generated.")
}

func main() {

	flag.Usage = func() {
		_, _ = fmt.Fprint(os.Stdout, `
Usage of jet:
  -host string
        Database host path (Example: localhost)
  -port int
        Database port
  -user string
        Database user
  -password string
        The user’s password
  -dbname string
        name of the database
  -params string
        Additional connection string parameters(optional)
  -schema string
        Database schema name. (default "public")
  -sslmode string
        Whether or not to use SSL(optional) (default "disable")
  -path string
        Destination dir for files generated.
`)
	}

	flag.Parse()

	var err error

	switch source {
	case jet.PostgreSQL.Name:
		if host == "" || port == 0 || user == "" || dbName == "" || schemaName == "" {
			fmt.Println("\njet: required flag missing")
			flag.Usage()
			os.Exit(-2)
		}

		genData := postgres.DBConnection{
			Host:     host,
			Port:     port,
			User:     user,
			Password: password,
			SslMode:  sslmode,
			Params:   params,

			DBName:     dbName,
			SchemaName: schemaName,
		}

		err = postgres.Generate(destDir, genData)

	case jet.MySQL.Name:
		if host == "" || port == 0 || user == "" || dbName == "" {
			fmt.Println("\njet: required flag missing")
			flag.Usage()
			os.Exit(-2)
		}

		dbConn := mysql.DBConnection{
			Host:     host,
			Port:     port,
			User:     user,
			Password: password,
			SslMode:  sslmode,
			Params:   params,
			DBName:   dbName,
		}

		err = mysql.Generate(destDir, dbConn)
	}

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
}
