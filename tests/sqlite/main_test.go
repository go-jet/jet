package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	"github.com/pkg/profile"
	"github.com/stretchr/testify/require"
	"os"
	"runtime"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

var db *sqlite.DB
var sampleDB *sqlite.DB

var skipStatementsCaching bool

func init() {
	skipStatementsCaching = os.Getenv("JET_TESTS_NO_STMT_CACHE") == "true"
}

func TestMain(m *testing.M) {
	defer profile.Start().Stop()

	for _, cachingEnabled := range []bool{false, true} {

		if cachingEnabled && skipStatementsCaching {
			continue //skipped by global env variable
		}

		func() {

			sqlDB, err := sql.Open("sqlite3", "file:"+dbconfig.SakilaDBPath)
			throw.OnError(err)
			db = sqlite.NewDB(sqlDB).WithStatementsCaching(cachingEnabled)
			defer db.Close()

			_, err = db.Exec(fmt.Sprintf("ATTACH DATABASE '%s' as 'chinook';", dbconfig.ChinookDBPath))
			throw.OnError(err)

			sqlSampleDB, err := sql.Open("sqlite3", dbconfig.TestSampleDBPath)
			throw.OnError(err)
			sampleDB = sqlite.NewDB(sqlSampleDB).WithStatementsCaching(cachingEnabled)
			defer sampleDB.Close()

			ret := m.Run()
			if ret != 0 {
				os.Exit(ret)
			}

		}()

	}

}

var loggedSQL string
var loggedSQLArgs []interface{}
var loggedDebugSQL string

var queryInfo sqlite.QueryInfo
var callerFile string
var callerLine int
var callerFunction string

func init() {
	sqlite.SetLogger(func(ctx context.Context, statement sqlite.PrintableStatement) {
		loggedSQL, loggedSQLArgs = statement.Sql()
		loggedDebugSQL = statement.DebugSql()
	})

	sqlite.SetQueryLogger(func(ctx context.Context, info sqlite.QueryInfo) {
		queryInfo = info
		callerFile, callerLine, callerFunction = info.Caller()
	})
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

func requireLogged(t *testing.T, statement sqlite.Statement) {
	query, args := statement.Sql()
	require.Equal(t, loggedSQL, query)
	require.Equal(t, loggedSQLArgs, args)
	require.Equal(t, loggedDebugSQL, statement.DebugSql())
}

func beginSampleDBTx(t *testing.T) *sqlite.Tx {
	tx, err := sampleDB.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	return tx
}

func beginDBTx(t *testing.T) *sqlite.Tx {
	tx, err := db.Begin()
	require.NoError(t, err)
	return tx
}
