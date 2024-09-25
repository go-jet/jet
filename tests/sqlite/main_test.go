package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/sqlite"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pkg/profile"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB
var sampleDB *sql.DB
var testRoot string

func TestMain(m *testing.M) {
	rand.Seed(time.Now().Unix())
	defer profile.Start().Stop()

	setTestRoot()

	var err error
	db, err = sql.Open("sqlite3", "file:"+SakilaDBPath)
	throw.OnError(err)
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("ATTACH DATABASE '%s' as 'chinook';", ChinookDBPath))
	throw.OnError(err)

	sampleDB, err = sql.Open("sqlite3", TestSampleDBPath)
	throw.OnError(err)

	ret := m.Run()

	if ret != 0 {
		os.Exit(ret)
	}
}

func setTestRoot() {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	byteArr, err := cmd.Output()
	if err != nil {
		panic(err)
	}

	testRoot = strings.TrimSpace(string(byteArr)) + "/tests/"
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

func beginSampleDBTx(t *testing.T) *sql.Tx {
	tx, err := sampleDB.Begin()
	require.NoError(t, err)
	return tx
}

func beginDBTx(t *testing.T) *sql.Tx {
	tx, err := db.Begin()
	require.NoError(t, err)
	return tx
}
