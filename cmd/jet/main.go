package main

import (
	"flag"
	"fmt"
	sqlitegen "github.com/go-jet/jet/v2/generator/sqlite"
	"os"
	"strings"

	mysqlgen "github.com/go-jet/jet/v2/generator/mysql"
	postgresgen "github.com/go-jet/jet/v2/generator/postgres"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var (
	source string

	dsn        string
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
	flag.StringVar(&source, "source", "", "Database system name (PostgreSQL, MySQL, MariaDB or SQLite)")

	flag.StringVar(&dsn, "dsn", "", "Data source name connection string (Example: postgresql://user@localhost:5432/otherdb?sslmode=trust)")
	flag.StringVar(&host, "host", "", "Database host path (Example: localhost)")
	flag.IntVar(&port, "port", 0, "Database port")
	flag.StringVar(&user, "user", "", "Database user")
	flag.StringVar(&password, "password", "", "The user’s password")
	flag.StringVar(&params, "params", "", "Additional connection string parameters(optional)")
	flag.StringVar(&dbName, "dbname", "", "Database name")
	flag.StringVar(&schemaName, "schema", "public", `Database schema name. (default "public") (ignored for MySQL and MariaDB)`)
	flag.StringVar(&sslmode, "sslmode", "disable", `Whether or not to use SSL(optional)(default "disable") (ignored for MySQL and MariaDB)`)

	flag.StringVar(&destDir, "path", "", "Destination dir for files generated.")
}

func main() {

	flag.Usage = func() {
		_, _ = fmt.Fprint(os.Stdout, `
Jet generator 2.6.0

Usage:
  -dsn string
    	Data source name. Unified format for connecting to database.
    	PostgreSQL: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
		Example:
			postgresql://user:pass@localhost:5432/dbname
    	MySQL: https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html
		Example:
			mysql://jet:jet@tcp(localhost:3306)/dvds
    	SQLite: https://www.sqlite.org/c3ref/open.html#urifilenameexamples
		Example:
			file://path/to/database/file
  -source string
    	Database system name (PostgreSQL, MySQL, MariaDB or SQLite)
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
        Database schema name. (default "public") (ignored for MySQL, MariaDB and SQLite)
  -sslmode string
        Whether or not to use SSL(optional) (default "disable") (ignored for MySQL, MariaDB and SQLite)
  -path string
        Destination dir for files generated.

Example commands:

	$ jet -source=PostgreSQL -dbname=jetdb -host=localhost -port=5432 -user=jet -password=jet -schema=dvds -path=./gen
	$ jet -dsn=postgresql://jet:jet@localhost:5432/jetdb -schema=dvds -path=./gen
	$ jet -source=postgres -dsn="user=jet password=jet host=localhost port=5432 dbname=jetdb" -schema=dvds -path=./gen
	$ jet -source=sqlite -dsn="file://path/to/sqlite/database/file" -schema=dvds -path=./gen
`)
	}

	flag.Parse()

	if dsn == "" {
		// validations for separated connection flags.
		if source == "" || host == "" || port == 0 || user == "" || dbName == "" {
			printErrorAndExit("ERROR: required flag(s) missing")
		}
	} else {
		if source == "" {
			// try to get source from schema
			source = detectSchema(dsn)
		}

		// validations when dsn != ""
		if source == "" {
			printErrorAndExit("ERROR: required -source flag missing.")
		}
	}

	var err error

	switch strings.ToLower(strings.TrimSpace(source)) {
	case "postgresql", "postgres":
		if dsn != "" {
			err = postgresgen.GenerateDSN(dsn, schemaName, destDir)
			break
		}
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

	case "mysql", "mysqlx", "mariadb":
		if dsn != "" {
			err = mysqlgen.GenerateDSN(dsn, destDir)
			break
		}
		dbConn := mysqlgen.DBConnection{
			Host:     host,
			Port:     port,
			User:     user,
			Password: password,
			Params:   params,
			DBName:   dbName,
		}

		err = mysqlgen.Generate(destDir, dbConn)
	case "sqlite":
		if dsn == "" {
			printErrorAndExit("ERROR: required -dsn flag missing.")
		}
		err = sqlitegen.GenerateDSN(dsn, destDir)
	default:
		printErrorAndExit("ERROR: unknown data source " + source + ". Only postgres, mysql, mariadb and sqlite are supported.")
	}

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-5)
	}
}

func printErrorAndExit(error string) {
	fmt.Println("\n", error)
	flag.Usage()
	os.Exit(-2)
}

func detectSchema(dsn string) string {
	match := strings.SplitN(dsn, "://", 2)
	if len(match) < 2 { // not found
		return ""
	}

	protocol := match[0]

	if protocol == "file" {
		return "sqlite"
	}

	return match[0]
}
