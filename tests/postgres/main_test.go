package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/stmtcache"
	"github.com/go-jet/jet/v2/tests/internal/utils/repo"
	"github.com/jackc/pgx/v4/stdlib"
	"os"
	"runtime"
	"testing"

	"github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	_ "github.com/lib/pq"
	"github.com/pkg/profile"
	"github.com/stretchr/testify/require"

	_ "github.com/jackc/pgx/v4/stdlib"
)

var db *stmtcache.DB
var testRoot string

var source string
var withStatementCaching bool

const CockroachDB = "COCKROACH_DB"

func init() {
	source = os.Getenv("PG_SOURCE")
	withStatementCaching = os.Getenv("JET_TESTS_WITH_STMT_CACHE") == "true"
}

func sourceIsCockroachDB() bool {
	return source == CockroachDB
}

func skipForCockroachDB(t *testing.T) {
	if sourceIsCockroachDB() {
		t.SkipNow()
	}
}

func TestMain(m *testing.M) {
	defer profile.Start().Stop()

	setTestRoot()

	for _, driverName := range []string{"postgres", "pgx"} {

		fmt.Printf("\nRunning postgres tests for driver: %s, caching enabled: %t \n", driverName, withStatementCaching)

		func() {
			sqlDB, err := sql.Open(driverName, getConnectionString())
			if err != nil {
				fmt.Println(err.Error())
				panic("Failed to connect to test db")
			}
			db = stmtcache.New(sqlDB).SetCaching(withStatementCaching)
			defer func(db *stmtcache.DB) {
				err := db.Close()
				if err != nil {
					fmt.Printf("ERROR: Failed to close db connection, %v", err)
				}
			}(db)

			for i := 0; i < runCount(withStatementCaching); i++ {
				ret := m.Run()
				if ret != 0 {
					fmt.Printf("\nFAIL: Running postgres tests failed for driver: %s, caching enabled: %t \n", driverName, withStatementCaching)
					os.Exit(ret)
				}
			}
		}()
	}

}

func runCount(stmtCaching bool) int {
	if stmtCaching {
		return 2
	}

	return 1
}

func getConnectionString() string {
	if sourceIsCockroachDB() {
		return dbconfig.CockroachConnectString
	}

	return dbconfig.PostgresConnectString
}

func setTestRoot() {
	testRoot = repo.GetTestsDirPath()
}

var loggedSQL string
var loggedSQLArgs []interface{}
var loggedDebugSQL string

var queryInfo postgres.QueryInfo
var callerFile string
var callerLine int
var callerFunction string

func init() {
	postgres.SetLogger(func(ctx context.Context, statement postgres.PrintableStatement) {
		loggedSQL, loggedSQLArgs = statement.Sql()
		loggedDebugSQL = statement.DebugSql()
	})

	postgres.SetQueryLogger(func(ctx context.Context, info postgres.QueryInfo) {
		queryInfo = info
		callerFile, callerLine, callerFunction = info.Caller()
	})
}

func requireLogged(t *testing.T, statement postgres.Statement) {
	query, args := statement.Sql()
	require.Equal(t, loggedSQL, query)
	require.Equal(t, loggedSQLArgs, args)
	require.Equal(t, loggedDebugSQL, statement.DebugSql())
}

func requireQueryLogged(t *testing.T, statement postgres.Statement, rowsProcessed int64) {
	query, args := statement.Sql()
	queryLogged, argsLogged := queryInfo.Statement.Sql()

	require.Equal(t, query, queryLogged)
	require.Equal(t, args, argsLogged)
	require.Equal(t, queryInfo.RowsProcessed, rowsProcessed)

	pc, file, _, _ := runtime.Caller(1)
	funcDetails := runtime.FuncForPC(pc)
	require.Equal(t, file, callerFile)
	require.NotEmpty(t, callerLine)
	require.Equal(t, funcDetails.Name(), callerFunction)
}

func skipForPgxDriver(t *testing.T) {
	if isPgxDriver() {
		t.SkipNow()
	}
}

func isPgxDriver() bool {
	switch db.Driver().(type) {
	case *stdlib.Driver:
		return true
	}

	return false
}
