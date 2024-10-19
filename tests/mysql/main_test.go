package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/mysql"
	jetmysql "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/stmtcache"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"runtime"

	"github.com/pkg/profile"
	"os"
	"testing"
)

var db *stmtcache.DB

var source string
var withStatementCaching bool

const MariaDB = "MariaDB"

func init() {
	source = os.Getenv("MY_SQL_SOURCE")
	withStatementCaching = os.Getenv("JET_TESTS_WITH_STMT_CACHE") == "true"
}

func sourceIsMariaDB() bool {
	return source == MariaDB
}

func TestMain(m *testing.M) {
	defer profile.Start().Stop()

	func() {
		fmt.Printf("\nRunning mysql tests caching enabled: %t \n", withStatementCaching)

		sqlDB, err := sql.Open("mysql", dbconfig.MySQLConnectionString(sourceIsMariaDB(), ""))
		if err != nil {
			panic("Failed to connect to test db" + err.Error())
		}

		db = stmtcache.New(sqlDB).SetCaching(withStatementCaching)
		defer db.Close()

		for i := 0; i < runCount(withStatementCaching); i++ {
			ret := m.Run()
			if ret != 0 {
				fmt.Printf("\nFAIL: Running mysql tests failed, caching enabled: %t \n", withStatementCaching)
				os.Exit(ret)
			}
		}
	}()

}

func getConnectionString() string {
	return dbconfig.MySQLConnectionString(sourceIsMariaDB(), "")
}

func runCount(stmtCaching bool) int {
	if stmtCaching {
		return 3
	}

	return 1
}

var loggedSQL string
var loggedSQLArgs []interface{}
var loggedDebugSQL string

var queryInfo jetmysql.QueryInfo
var callerFile string
var callerLine int
var callerFunction string

func init() {
	jetmysql.SetLogger(func(ctx context.Context, statement jetmysql.PrintableStatement) {
		loggedSQL, loggedSQLArgs = statement.Sql()
		loggedDebugSQL = statement.DebugSql()
	})

	jetmysql.SetQueryLogger(func(ctx context.Context, info jetmysql.QueryInfo) {
		queryInfo = info
		callerFile, callerLine, callerFunction = info.Caller()
	})
}

func requireLogged(t *testing.T, statement mysql.Statement) {
	query, args := statement.Sql()
	require.Equal(t, loggedSQL, query)
	require.Equal(t, loggedSQLArgs, args)
	require.Equal(t, loggedDebugSQL, statement.DebugSql())
}

func requireQueryLogged(t *testing.T, statement mysql.Statement, rowsProcessed int64) {
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

func skipForMariaDB(t *testing.T) {
	if sourceIsMariaDB() {
		t.SkipNow()
	}
}

func onlyMariaDB(t *testing.T) {
	if !sourceIsMariaDB() {
		t.SkipNow()
	}
}
