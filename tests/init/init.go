package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/go-jet/jet/v2/generator/mysql"
	"github.com/go-jet/jet/v2/generator/sqlite"
	"github.com/go-jet/jet/v2/tests/internal/utils/repo"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/go-jet/jet/v2/generator/postgres"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	_ "github.com/mattn/go-sqlite3"
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
		initMySQLDB(testSuite == "mariadb")
		return
	}

	if testSuite == "sqlite" {
		initSQLiteDB()
		return
	}

	initPostgresDB()
	initMySQLDB(false)
	initMySQLDB(true)
	initSQLiteDB()
}

func initSQLiteDB() {
	err := sqlite.GenerateDSN(dbconfig.SakilaDBPath, repo.GetTestsFilePath("./.gentestdata/sqlite/sakila"))
	throw.OnError(err)
	err = sqlite.GenerateDSN(dbconfig.ChinookDBPath, repo.GetTestsFilePath("./.gentestdata/sqlite/chinook"))
	throw.OnError(err)
	err = sqlite.GenerateDSN(dbconfig.TestSampleDBPath, repo.GetTestsFilePath("./.gentestdata/sqlite/test_sample"))
	throw.OnError(err)
}

func initMySQLDB(isMariaDB bool) {

	mySQLDBs := []string{
		"dvds",
		"dvds2",
		"test_sample",
	}

	for _, dbName := range mySQLDBs {
		host := dbconfig.MySqLHost
		port := dbconfig.MySQLPort
		user := dbconfig.MySQLUser
		pass := dbconfig.MySQLPassword

		if isMariaDB {
			host = dbconfig.MariaDBHost
			port = dbconfig.MariaDBPort
			user = dbconfig.MariaDBUser
			pass = dbconfig.MariaDBPassword
		}

		cmdLine := fmt.Sprintf("mysql -h %s -P %d -u %s -p%s %s < %s", host, port, user, pass, dbName,
			"./testdata/init/mysql/"+dbName+".sql")

		fmt.Println(cmdLine)

		cmd := exec.Command("sh", "-c", cmdLine)

		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout

		err := cmd.Run()
		throw.OnError(err)

		err = mysql.Generate("./.gentestdata/mysql", mysql.DBConnection{
			Host:     host,
			Port:     port,
			User:     user,
			Password: pass,
			DBName:   dbName,
		})

		throw.OnError(err)
	}
}

func initPostgresDB() {
	db, err := sql.Open("postgres", dbconfig.PostgresConnectString)
	if err != nil {
		panic("Failed to connect to test db: " + err.Error())
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
			Host:       dbconfig.PgHost,
			Port:       dbconfig.PgPort,
			User:       dbconfig.PgUser,
			Password:   dbconfig.PgPassword,
			DBName:     dbconfig.PgDBName,
			SchemaName: schemaName,
			SslMode:    "disable",
		})
		throw.OnError(err)
	}
}

func execFile(db *sql.DB, sqlFilePath string) {
	testSampleSql, err := ioutil.ReadFile(sqlFilePath)
	throw.OnError(err)

	_, err = db.Exec(string(testSampleSql))
	throw.OnError(err)
}

func printOnError(err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}
