package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/go-jet/jet/generator/mysql"
	"github.com/go-jet/jet/generator/postgres"
	"github.com/go-jet/jet/tests/dbconfig"
	_ "github.com/lib/pq"
	"io/ioutil"
	"os"
	"os/exec"
)

var testSuite string

func init() {
	flag.StringVar(&testSuite, "testsuite", "all", "Test suite name (postgres or mysql)")

	flag.Parse()
}

func main() {

	if testSuite == "postgres" {
		initPostgresDB()
		return
	}

	if testSuite == "mysql" {
		initMySQLDB()
		return
	}

	initMySQLDB()
	initPostgresDB()
}

func initMySQLDB() {

	mySQLDBs := []string{
		"test_sample",
	}

	for _, dbName := range mySQLDBs {
		cmdLine := fmt.Sprintf("mysql -u %s -p%s %s < %s",
			dbconfig.MySQLUser, dbconfig.MySQLPassword, dbName, "./init/data/mysql/"+dbName+".sql")
		cmd := exec.Command("sh", "-c", cmdLine)

		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout

		err := cmd.Run()
		panicOnError(err)

		err = mysql.Generate("./.gentestdata/mysql", mysql.DBConnection{
			Host:     dbconfig.MySqLHost,
			Port:     dbconfig.MySQLPort,
			User:     dbconfig.MySQLUser,
			Password: dbconfig.MySQLPassword,
			//SslMode:
			//Params
			DBName: dbName,
		})

		panicOnError(err)
	}

}

func initPostgresDB() {
	db, err := sql.Open("postgres", dbconfig.PostgresConnectString)
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

		execFile(db, "./init/data/postgres/"+schemaName+".sql")

		err = postgres.Generate("./.gentestdata", postgres.DBConnection{
			Host:       dbconfig.Host,
			Port:       5432,
			User:       dbconfig.User,
			Password:   dbconfig.Password,
			DBName:     dbconfig.DBName,
			SchemaName: schemaName,
			SslMode:    "disable",
		})
		panicOnError(err)
	}
}

func execFile(db *sql.DB, sqlFilePath string) {
	testSampleSql, err := ioutil.ReadFile(sqlFilePath)
	panicOnError(err)

	_, err = db.Exec(string(testSampleSql))
	panicOnError(err)
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
