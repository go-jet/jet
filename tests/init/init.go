package main

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/generator/postgres"
	"github.com/go-jet/jet/tests/dbconfig"
	"io/ioutil"
)

func main() {
	fmt.Println(dbconfig.ConnectString)

	db, err := sql.Open("postgres", dbconfig.ConnectString)
	if err != nil {
		panic("Failed to connect to test db")
	}
	defer func() {
		err := db.Close()
		printOnError(err)
	}()

	schemaNames := []string{
		"dvds",
		"test_sample",
		"chinook",
		"northwind",
	}

	for _, schemaName := range schemaNames {
		testSampleSql, err := ioutil.ReadFile("./init/data/" + schemaName + ".sql")

		panicOnError(err)

		_, err = db.Exec(string(testSampleSql))

		panicOnError(err)

		err = postgres.Generate("./.gentestdata", postgres.DBConnection{
			Host:       dbconfig.Host,
			Port:       "5432",
			User:       dbconfig.User,
			Password:   dbconfig.Password,
			DBName:     dbconfig.DBName,
			SchemaName: schemaName,
			SslMode:    "disable",
		})

		panicOnError(err)
	}
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func printOnError(err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}
