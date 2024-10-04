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
	"github.com/go-jet/jet/v2/tests/internal/utils/containers"
	dbTools "github.com/go-jet/jet/v2/tests/internal/utils/db"
	"github.com/go-jet/jet/v2/tests/internal/utils/repo"
	mysqlTest "github.com/go-jet/jet/v2/tests/mysql"
	sqlite2 "github.com/go-jet/jet/v2/tests/sqlite"
	"log/slog"
	"os"
	"path"
	"strings"
	"time"

	pgTest "github.com/go-jet/jet/v2/tests/postgres"
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

// timeMethod records duration to execute oper function
func timeMethod(msg string, oper func() error) error {
	start := time.Now()
	err := oper()
	slog.Info(msg, slog.Any("Duration", time.Since(start)))
	return err

}

func main() {
	var err error

	switch strings.ToLower(testSuite) {
	case Postgres:
		err = timeMethod("postgres Schema Generation complete", func() error {
			return initPostgresDB(Postgres)
		})
	case Cockroach:
		err = timeMethod("couchDB Schema Generation complete", func() error {
			return initPostgresDB(Cockroach)
		})
	case MySql:
		err = timeMethod("Mysql Schema Generation complete", func() error {
			return initMySQLDB(MySql)
		})
	case MariaDB:
		err = timeMethod("MariaDB Schema Generation complete", func() error {
			return initMySQLDB(MariaDB)
		})
	case Sqlite:
		err = timeMethod("Sqlite Schema Generation complete", func() error {
			return initSQLiteDB()
		})
	case "all":
		err = timeMethod("couchDB Schema Generation complete", func() error {
			return initPostgresDB(Cockroach)
		})
		if err != nil {
			break
		}

		err = timeMethod("postgres Schema Generation complete", func() error {
			return initPostgresDB(Postgres)
		})
		if err != nil {
			break
		}

		err = timeMethod("Mysql Schema Generation complete", func() error {
			return initMySQLDB(MySql)
		})
		if err != nil {
			break
		}

		err = timeMethod("MariaDB Schema Generation complete", func() error {
			return initMySQLDB(MariaDB)
		})
		if err != nil {
			break
		}

		err = timeMethod("Sqlite Schema Generation complete", func() error {
			return initSQLiteDB()
		})
	default:
		panic("invalid testsuite flag. Test suite name (postgres, mysql, mariadb, cockroach, sqlite or all)")
	}

	if err != nil {
		fmt.Println(errfmt.Trace(err))
		os.Exit(1)
	}
}

func initSQLiteDB() error {
	err := sqlite.GenerateDSN(sqlite2.SakilaDBPath, repo.GetTestsFilePath("./.gentestdata/sqlite/sakila"))
	if err != nil {
		return fmt.Errorf("failed to generate sqlite sakila database types: %w", err)
	}
	err = sqlite.GenerateDSN(sqlite2.ChinookDBPath, repo.GetTestsFilePath("./.gentestdata/sqlite/chinook"))
	if err != nil {
		return fmt.Errorf("failed to generate sqlite chinook database types: %w", err)
	}
	err = sqlite.GenerateDSN(sqlite2.TestSampleDBPath, repo.GetTestsFilePath("./.gentestdata/sqlite/test_sample"))
	if err != nil {
		return fmt.Errorf("failed to generate sqlite test_sample database types: %w", err)
	}

	return nil
}

func initMySQLDB(dbType string) error {
	repoDir := repo.GetTestsDirPath()
	var (
		host     string
		port     int
		cancelFn context.CancelFunc
	)

	if dbType == MySql {
		host, port, cancelFn = containers.SetupWithMySQL(repoDir)
		mysqlTest.MySQLPort = port
		mysqlTest.MySqLHost = host
	} else {
		host, port, cancelFn = containers.SetupWithMariaDB(repoDir)
		mysqlTest.MariaDBPort = port
		mysqlTest.MariaDBHost = host
	}
	if cancelFn != nil {
		defer cancelFn()
	}

	mySQLDBs := []string{
		"dvds",
		"dvds2",
		"test_sample",
	}

	for _, dbName := range mySQLDBs {
		host := mysqlTest.MySqLHost
		port := mysqlTest.MySQLPort
		user := mysqlTest.MySQLUser
		pass := mysqlTest.MySQLPassword

		if dbType == MariaDB {
			host = mysqlTest.MariaDBHost
			port = mysqlTest.MariaDBPort
			user = mysqlTest.MariaDBUser
			pass = mysqlTest.MariaDBPassword
		}

		err := mysql.Generate(path.Join(repoDir, "./.gentestdata/mysql"), mysql.DBConnection{
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

func initPostgresDB(dbType string) error {
	repoDir := repo.GetTestsDirPath()
	var (
		connectionString string
		host             string
		port             int
		cancelFn         context.CancelFunc
	)

	if dbType == Postgres {
		host, port, cancelFn = containers.SetupWithPostgres(repoDir)
	} else {
		host, port, cancelFn = containers.SetupWithCockroach(repoDir)
	}
	connectionString = pgTest.PgConnectionString(host, port, pgTest.PgUser, pgTest.PgPassword, pgTest.PgDBName)
	if cancelFn != nil {
		defer cancelFn()
	}
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

		err = dbTools.ExecFile(db, path.Join(repoDir, fmt.Sprintf("./testdata/init/%s/%s.sql", dbType, schemaName)))
		if err != nil {
			return fmt.Errorf("failed to execute sql file: %w", err)
		}

		err = postgres.GenerateDSN(connectionString, schemaName, path.Join(repoDir, "./.gentestdata"))
		if err != nil {
			return fmt.Errorf("failed to generate jet types: %w", err)
		}
	}

	return nil
}
