package sqlite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/stmtcache"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	"github.com/pkg/profile"
	"github.com/stretchr/testify/require"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
)

var db *stmtcache.DB
var sampleDB *stmtcache.DB

var withStatementCaching bool

func init() {
	withStatementCaching = os.Getenv("JET_TESTS_WITH_STMT_CACHE") == "true"
}

func TestMain(m *testing.M) {
	defer profile.Start().Stop()

	func() {
		fmt.Printf("\nRunning sqlite tests caching enabled: %t \n", withStatementCaching)

		sqlDB, err := sql.Open("sqlite3", "file:"+dbconfig.SakilaDBPath)
		throw.OnError(err)
		db = stmtcache.New(sqlDB).SetCaching(withStatementCaching)
		defer db.Close()

		_, err = db.Exec(fmt.Sprintf("ATTACH DATABASE '%s' as 'chinook';", dbconfig.ChinookDBPath))
		throw.OnError(err)

		sql.Register("sqlite3_base64", &sqlite3.SQLiteDriver{
			ConnectHook: func(sc *sqlite3.SQLiteConn) error {
				return sc.RegisterFunc("BASE64_ENCODE", func(input any) any {
					_, ok := input.(string)
					if !ok {
						return nil
					}
					value := fmt.Sprintf("%v", input)
					return base64.StdEncoding.EncodeToString([]byte(value))
				}, true)
			},
		})

		sqlSampleDB, err := sql.Open("sqlite3_base64", dbconfig.TestSampleDBPath)

		throw.OnError(err)
		sampleDB = stmtcache.New(sqlSampleDB).SetCaching(withStatementCaching)

		defer sampleDB.Close()

		for i := 0; i < runCount(withStatementCaching); i++ {
			ret := m.Run()
			if ret != 0 {
				fmt.Printf("\nFAIL: Running sqlite tests failed, caching enabled: %t \n", withStatementCaching)
				os.Exit(ret)
			}
		}

	}()

}

func allowUnmappedFields(f func()) {
	previous := qrm.GlobalConfig.StrictFieldMapping
	defer func() { qrm.GlobalConfig.StrictFieldMapping = previous }()
	qrm.GlobalConfig.StrictFieldMapping = false
	f()
}

func requireStrictFieldMapping(f func()) {
	previous := qrm.GlobalConfig.StrictFieldMapping
	defer func() { qrm.GlobalConfig.StrictFieldMapping = previous }()
	qrm.GlobalConfig.StrictFieldMapping = true
	f()
}

func runCount(stmtCaching bool) int {
	if stmtCaching {
		return 4
	}

	return 1
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

func requireQueryLogged(t *testing.T, statement sqlite.Statement, rowsProcessed int64) {
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

func beginSampleDBTx(t *testing.T) *stmtcache.Tx {
	tx, err := sampleDB.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	return tx
}

func beginDBTx(t *testing.T) *stmtcache.Tx {
	tx, err := db.Begin()
	require.NoError(t, err)
	return tx
}
