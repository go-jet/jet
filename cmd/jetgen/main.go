package main

import (
	"flag"
	"fmt"
	"github.com/go-jet/jet/generator/postgresgen"
	"os"
)

var (
	host       string
	port       string
	user       string
	password   string
	sslmode    string
	params     string
	dbName     string
	schemaName string

	destDir string
)

func init() {
	flag.StringVar(&host, "host", "", "Database host path (Example: localhost)")
	flag.StringVar(&port, "port", "", "Database port")
	flag.StringVar(&user, "user", "", "Database user")
	flag.StringVar(&password, "password", "", "The user’s password")
	flag.StringVar(&sslmode, "sslmode", "disable", "Whether or not to use SSL")
	flag.StringVar(&params, "params", "", "Additional connection string parameters.")

	flag.StringVar(&dbName, "dbname", "", "name of the database")
	flag.StringVar(&schemaName, "schema", "public", "Database schema name.")

	flag.StringVar(&destDir, "path", "", "Destination dir for generated files.")

	flag.Parse()
}

func main() {

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

	err := postgresgen.Generate(destDir, genData)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
}
