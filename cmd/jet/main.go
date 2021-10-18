package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	mysqlgen "github.com/go-jet/jet/v2/generator/mysql"
	postgresgen "github.com/go-jet/jet/v2/generator/postgres"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/postgres"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
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
	exclude string
)

func init() {
	flag.StringVar(&source, "source", "", "Database system name (PostgreSQL, MySQL or MariaDB)")

	flag.StringVar(&host, "host", "", "Database host path (Example: localhost)")
	flag.IntVar(&port, "port", 0, "Database port")
	flag.StringVar(&user, "user", "", "Database user")
	flag.StringVar(&password, "password", "", "The user’s password")
	flag.StringVar(&params, "params", "", "Additional connection string parameters(optional)")
	flag.StringVar(&dbName, "dbname", "", "Database name")
	flag.StringVar(&schemaName, "schema", "public", `Database schema name. (default "public") (ignored for MySQL and MariaDB)`)
	flag.StringVar(&sslmode, "sslmode", "disable", `Whether or not to use SSL(optional)(default "disable") (ignored for MySQL and MariaDB)`)

	flag.StringVar(&destDir, "path", "", "Destination dir for files generated")
	flag.StringVar(&exclude, "exclude", "", "Comma separated list of tables to ignore")
}

func main() {

	flag.Usage = func() {
		_, _ = fmt.Fprint(os.Stdout, `
Jet generator 2.5.0

Usage:
  -source string
    	Database system name (PostgreSQL, MySQL or MariaDB)
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
        Database schema name. (default "public") (ignored for MySQL and MariaDB)
  -sslmode string
        Whether or not to use SSL(optional) (default "disable") (ignored for MySQL and MariaDB)
  -path string
        Destination dir for files generated
  -exclude string
        Comma separated list of tables to ignore
`)
	}

	flag.Parse()

	if source == "" || host == "" || port == 0 || user == "" || dbName == "" {
		printErrorAndExit("\nERROR: required flag(s) missing")
	}

	config := utils.Config{
		Exclude: strings.Split(exclude, ","),
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

		err = postgresgen.Generate(config, destDir, genData)

	case strings.ToLower(mysql.Dialect.Name()), "mariadb":

		dbConn := mysqlgen.DBConnection{
			Host:     host,
			Port:     port,
			User:     user,
			Password: password,
			Params:   params,
			DBName:   dbName,
		}

		err = mysqlgen.Generate(config, destDir, dbConn)
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
