package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/go-jet/jet/v2/generator/mysql"
	"github.com/go-jet/jet/v2/generator/postgres"
	"github.com/go-jet/jet/v2/generator/sqlite"
	"github.com/go-jet/jet/v2/tests/internal/utils/repo"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v4/stdlib"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var testSuite string

func init() {
	flag.StringVar(&testSuite, "testsuite", "all", "Test suite name (postgres, mysql, mariadb, cockroach, sqlite or all)")
	flag.Parse()
}

// Database names
const (
	Postgres  = "postgres"
	MySql     = "mysql"
	MariaDB   = "mariadb"
	Sqlite    = "sqlite"
	Cockroach = "cockroach"
)

func main() {

	switch strings.ToLower(testSuite) {
	case Postgres:
		initPostgresDB(Postgres, dbconfig.PostgresConnectString)
	case Cockroach:
		initPostgresDB(Cockroach, dbconfig.CockroachConnectString)
	case MySql:
		initMySQLDB(false)
	case MariaDB:
		initMySQLDB(true)
	case Sqlite:
		initSQLiteDB()
	case "all":
		initPostgresDB(Cockroach, dbconfig.CockroachConnectString)
		initPostgresDB(Postgres, dbconfig.PostgresConnectString)
		initMySQLDB(false)
		initMySQLDB(true)
		initSQLiteDB()
	default:
		panic("invalid testsuite flag. Test suite name (postgres, mysql, mariadb, cockroach, sqlite or all)")
	}
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

func initPostgresDB(dbType string, connectionString string) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic("Failed to connect to test db: " + err.Error())
	}
	defer func() {
		err := db.Close()
		printOnError(err)
	}()

	schemaNames := []string{
		"northwind",
		"dvds",
		"test_sample",
		"chinook",
		"chinook2",
	}

	for _, schemaName := range schemaNames {
		fmt.Println("\nInitializing", schemaName, "schema...")

		execFile(db, fmt.Sprintf("./testdata/init/%s/%s.sql", dbType, schemaName))

		err = postgres.GenerateDSN(connectionString, schemaName, "./.gentestdata")
		throw.OnError(err)
	}
}

func execFile(db *sql.DB, sqlFilePath string) {
	testSampleSql, err := ioutil.ReadFile(sqlFilePath)
	throw.OnError(err)

	err = execInTx(db, func(tx *sql.Tx) error {
		_, err := tx.Exec(string(testSampleSql))
		return err
	})
	throw.OnError(err)
}

func execInTx(db *sql.DB, f func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadUncommitted, // to speed up initialization of test database
	})

	if err != nil {
		return err
	}

	err = f(tx)

	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func printOnError(err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}
