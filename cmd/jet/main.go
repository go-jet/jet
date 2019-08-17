package main

import (
	"flag"
	"fmt"
	mysqlgen "github.com/go-jet/jet/generator/mysql"
	postgresgen "github.com/go-jet/jet/generator/postgres"
	"github.com/go-jet/jet/mysql"
	"github.com/go-jet/jet/postgres"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"os"
	"strings"
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
	flag.StringVar(&source, "source", "", "Database system name (PostgreSQL or MySQL)")

	flag.StringVar(&host, "host", "", "Database host path (Example: localhost)")
	flag.IntVar(&port, "port", 0, "Database port")
	flag.StringVar(&user, "user", "", "Database user")
	flag.StringVar(&password, "password", "", "The user’s password")
	flag.StringVar(&params, "params", "", "Additional connection string parameters(optional)")
	flag.StringVar(&dbName, "dbname", "", "Database name")
	flag.StringVar(&schemaName, "schema", "public", `Database schema name. (default "public") (ignored for MySQL)`)
	flag.StringVar(&sslmode, "sslmode", "disable", `Whether or not to use SSL(optional)(default "disable") (ignored for MySQL)`)

	flag.StringVar(&destDir, "path", "", "Destination dir for files generated.")
}

func main() {

	flag.Usage = func() {
		_, _ = fmt.Fprint(os.Stdout, `
Jet generator 2.0.0

Usage:
  -source string
    	Database system name (PostgreSQL or MySQL)
  -host string
        Database host path (Example: localhost)
  -port int
        Database port
  -user string
        Database user
  -password string
        The user’s password
  -dbname string
        Database name
  -params string
        Additional connection string parameters(optional)
  -schema string
        Database schema name. (default "public") (ignored for MySQL)
  -sslmode string
        Whether or not to use SSL(optional) (default "disable") (ignored for MySQL)
  -path string
        Destination dir for files generated.
`)
	}

	flag.Parse()

	if source == "" || host == "" || port == 0 || user == "" || dbName == "" {
		printErrorAndExit("\nERROR: required flag(s) missing")
	}

	var err error

	switch strings.ToLower(strings.TrimSpace(source)) {
	case strings.ToLower(postgres.Dialect.Name()),
		strings.ToLower(postgres.Dialect.PackageName()):
		genData := postgresgen.DBConnection{
			Host:     host,
			Port:     port,
			User:     user,
			Password: password,
			SslMode:  sslmode,
			Params:   params,

			DBName:     dbName,
			SchemaName: schemaName,
		}

		err = postgresgen.Generate(destDir, genData)

	case strings.ToLower(mysql.Dialect.Name()):

		dbConn := mysqlgen.DBConnection{
			Host:     host,
			Port:     port,
			User:     user,
			Password: password,
			Params:   params,
			DBName:   dbName,
		}

		err = mysqlgen.Generate(destDir, dbConn)
	default:
		fmt.Println("ERROR: unsupported source " + source + ". " + postgres.Dialect.Name() + " and " + mysql.Dialect.Name() + " are currently supported.")
		os.Exit(-4)
	}

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-5)
	}
}

func printErrorAndExit(error string) {
	fmt.Println(error)
	flag.Usage()
	os.Exit(-2)
}
