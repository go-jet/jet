package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/go-jet/jet/v2/generator/mysql"
	"github.com/go-jet/jet/v2/generator/postgres"
	"github.com/go-jet/jet/v2/generator/sqlite"
	"github.com/go-jet/jet/v2/internal/utils/errfmt"
	"github.com/go-jet/jet/v2/tests/internal/utils/repo"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

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
	var err error

	switch strings.ToLower(testSuite) {
	case Postgres:
		err = initPostgresDB(Postgres, dbconfig.PostgresConnectString)
	case Cockroach:
		err = initPostgresDB(Cockroach, dbconfig.CockroachConnectString)
	case MySql:
		err = initMySQLDB(false)
	case MariaDB:
		err = initMySQLDB(true)
	case Sqlite:
		err = initSQLiteDB()
	case "all":
		err = initPostgresDB(Cockroach, dbconfig.CockroachConnectString)
		if err != nil {
			break
		}
		err = initPostgresDB(Postgres, dbconfig.PostgresConnectString)
		if err != nil {
			break
		}
		err = initMySQLDB(false)
		if err != nil {
			break
		}

		err = initMySQLDB(true)
		if err != nil {
			break
		}
		err = initSQLiteDB()
	default:
		panic("invalid testsuite flag. Test suite name (postgres, mysql, mariadb, cockroach, sqlite or all)")
	}

	if err != nil {
		fmt.Println(errfmt.Trace(err))
	}
}

func initSQLiteDB() error {
	err := sqlite.GenerateDSN(dbconfig.SakilaDBPath, repo.GetTestsFilePath("./.gentestdata/sqlite/sakila"))
	if err != nil {
		return fmt.Errorf("failed to generate sqlite sakila database types: %w", err)
	}
	err = sqlite.GenerateDSN(dbconfig.ChinookDBPath, repo.GetTestsFilePath("./.gentestdata/sqlite/chinook"))
	if err != nil {
		return fmt.Errorf("failed to generate sqlite chinook database types: %w", err)
	}
	err = sqlite.GenerateDSN(dbconfig.TestSampleDBPath, repo.GetTestsFilePath("./.gentestdata/sqlite/test_sample"))
	if err != nil {
		return fmt.Errorf("failed to generate sqlite test_sample database types: %w", err)
	}

	return nil
}

func initMySQLDB(isMariaDB bool) error {

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
		if err != nil {
			return fmt.Errorf("failed to initialize mysql database %s: %w", dbName, err)
		}

		err = mysql.Generate("./.gentestdata/mysql", mysql.DBConnection{
			Host:     host,
			Port:     port,
			User:     user,
			Password: pass,
			DBName:   dbName,
		})

		if err != nil {
			return fmt.Errorf("failed to generate jet types for '%s' database: %w", dbName, err)
		}
	}

	return nil
}

func initPostgresDB(dbType string, connectionString string) error {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return fmt.Errorf("failed to open '%s' db connection '%s': %w", dbType, connectionString, err)
	}
	defer db.Close()

	schemaNames := []string{
		"northwind",
		"dvds",
		"test_sample",
		"chinook",
		"chinook2",
	}

	for _, schemaName := range schemaNames {
		fmt.Println("\nInitializing", schemaName, "schema...")

		err = execFile(db, fmt.Sprintf("./testdata/init/%s/%s.sql", dbType, schemaName))
		if err != nil {
			return fmt.Errorf("failed to execute sql file: %w", err)
		}

		err = postgres.GenerateDSN(connectionString, schemaName, "./.gentestdata")
		if err != nil {
			return fmt.Errorf("failed to generate jet types: %w", err)
		}
	}

	return nil
}

func execFile(db *sql.DB, sqlFilePath string) error {
	testSampleSql, err := ioutil.ReadFile(sqlFilePath)
	if err != nil {
		return fmt.Errorf("failed to read sql file - %s: %w", sqlFilePath, err)
	}

	err = execInTx(db, func(tx *sql.Tx) error {
		_, err := tx.Exec(string(testSampleSql))
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to execute sql file - %s: %w", sqlFilePath, err)
	}

	return nil
}

func execInTx(db *sql.DB, f func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadUncommitted, // to speed up initialization of test database
	})
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	err = f(tx)

	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction")
	}

	return nil
}
