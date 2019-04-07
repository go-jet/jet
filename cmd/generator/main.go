package main

import (
	"flag"
	"fmt"
	"github.com/sub0zero/go-sqlbuilder/generator"
	"os"
)

var genDirPath string
var dbConnectionString string
var dbName string
var schemaName string

func init() {
	flag.StringVar(&genDirPath, "path", "", "Destination for generated files.")
	flag.StringVar(&dbConnectionString, "db", "", "Connection string to database server")
	flag.StringVar(&dbName, "dbName", "", "Name of the database")
	flag.StringVar(&schemaName, "schema", "public", "Database schema name.")

	flag.Parse()
}

func main() {

	fmt.Println(genDirPath, dbConnectionString, dbName, schemaName)

	err := generator.Generate(genDirPath, dbConnectionString, dbName, schemaName)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}

	fmt.Println("SUCCESS")
}
