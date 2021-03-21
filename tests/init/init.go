package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/go-jet/jet/v2/generator/mysql"
	"github.com/go-jet/jet/v2/generator/postgres"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)

var testSuite string

func init() {
	flag.StringVar(&testSuite, "testsuite", "all", "Test suite name (postgres or mysql)")

	flag.Parse()
}

func main() {

	testSuite = strings.ToLower(testSuite)

	if testSuite == "postgres" {
		initPostgresDB()
		return
	}

	if testSuite == "mysql" || testSuite == "mariadb" {
		initMySQLDB()
		return
	}

	initMySQLDB()
	initPostgresDB()
}

func initMySQLDB() {

	mySQLDBs := []string{
		"dvds",
		"dvds2",
		"test_sample",
	}

	for _, dbName := range mySQLDBs {
		cmdLine := fmt.Sprintf("mysql -h 127.0.0.1 -u %s -p%s %s < %s",
			dbconfig.MySQLUser, dbconfig.MySQLPassword, dbName, "./testdata/init/mysql/"+dbName+".sql")

		fmt.Println(cmdLine)

		cmd := exec.Command("sh", "-c", cmdLine)

		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout

		err := cmd.Run()
		utils.PanicOnError(err)

		err = mysql.Generate("./.gentestdata/mysql", mysql.DBConnection{
			Host:     dbconfig.MySqLHost,
			Port:     dbconfig.MySQLPort,
			User:     dbconfig.MySQLUser,
			Password: dbconfig.MySQLPassword,
			DBName:   dbName,
		})

		utils.PanicOnError(err)
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
		"chinook2",
		"northwind",
	}

	for _, schemaName := range schemaNames {

		execFile(db, "./testdata/init/postgres/"+schemaName+".sql")

		err = postgres.Generate("./.gentestdata", postgres.DBConnection{
			Host:       dbconfig.Host,
			Port:       5432,
			User:       dbconfig.User,
			Password:   dbconfig.Password,
			DBName:     dbconfig.DBName,
			SchemaName: schemaName,
			SslMode:    "disable",
		})
		utils.PanicOnError(err)
	}
}

func execFile(db *sql.DB, sqlFilePath string) {
	testSampleSql, err := ioutil.ReadFile(sqlFilePath)
	utils.PanicOnError(err)

	_, err = db.Exec(string(testSampleSql))
	utils.PanicOnError(err)
}

func printOnError(err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}
