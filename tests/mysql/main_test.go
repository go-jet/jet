package mysql

import (
	"context"
	"database/sql"
	jetmysql "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/internal/utils/containers"
	"github.com/go-jet/jet/v2/tests/internal/utils/repo"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"runtime"

	"github.com/pkg/profile"
	"os"
	"testing"
)

var (
	db       *sql.DB
	testRoot string
	source   string
)

func init() {
	source = os.Getenv(MySqlSourceEnvKey)
}

func sourceIsMariaDB() bool {
	return source == MariaDB
}

func TestMain(m *testing.M) {
	defer profile.Start().Stop()
	var (
		host   string
		port   int
		cancel context.CancelFunc
	)

	testRoot = repo.GetTestsDirPath()
	if sourceIsMariaDB() {
		host, port, cancel = containers.SetupWithMariaDB(testRoot)
		MariaDBHost = host
		MariaDBPort = port

	} else {
		host, port, cancel = containers.SetupWithMySQL(testRoot)
		MySqLHost = host
		MySQLPort = port
	}

	if cancel != nil {
		defer cancel()
	}

	var err error
	db, err = sql.Open("mysql", ConnectionString(sourceIsMariaDB(), ""))
	if err != nil {
		panic("Failed to connect to test db" + err.Error())
	}
	defer db.Close()

	ret := m.Run()

	os.Exit(ret)
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

func skipForMariaDB(t *testing.T) {
	if sourceIsMariaDB() {
		t.SkipNow()
	}
}
