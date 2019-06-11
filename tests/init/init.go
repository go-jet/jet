package main

import (
	"database/sql"
	"github.com/go-jet/jet/generator"
	"github.com/go-jet/jet/tests/dbconfig"
	"io/ioutil"
)

func main() {
	db, err := sql.Open("postgres", dbconfig.ConnectString)
	if err != nil {
		panic("Failed to connect to test db")
	}
	defer db.Close()

	testSampleSql, err := ioutil.ReadFile("./init/data/test_sample.sql")

	panicOnError(err)

	_, err = db.Exec(string(testSampleSql))

	panicOnError(err)

	dvdsSql, err := ioutil.ReadFile("./init/data/dvds.sql")

	panicOnError(err)

	_, err = db.Exec(string(dvdsSql))

	panicOnError(err)

	err = generator.Generate("./.test_files", generator.GeneratorData{
		Host:       dbconfig.Host,
		Port:       "5432",
		User:       dbconfig.User,
		Password:   dbconfig.Password,
		DBName:     dbconfig.DBName,
		SchemaName: "dvds",
	})

	panicOnError(err)

	err = generator.Generate("./.test_files", generator.GeneratorData{
		Host:       dbconfig.Host,
		Port:       "5432",
		User:       dbconfig.User,
		Password:   dbconfig.Password,
		DBName:     dbconfig.DBName,
		SchemaName: "test_sample",
	})

	panicOnError(err)
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
