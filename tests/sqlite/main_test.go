package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"os/exec"
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
	db, err = sql.Open("sqlite3", "file:"+dbconfig.SakilaDBPath)
	throw.OnError(err)

	_, err = db.Exec(fmt.Sprintf("ATTACH DATABASE '%s' as 'chinook';", dbconfig.ChinookDBPath))
	throw.OnError(err)

	sampleDB, err = sql.Open("sqlite3", dbconfig.TestSampleDBPath)
	throw.OnError(err)

	defer db.Close()

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

func init() {
	sqlite.SetLogger(func(ctx context.Context, statement sqlite.PrintableStatement) {
		loggedSQL, loggedSQLArgs = statement.Sql()
		loggedDebugSQL = statement.DebugSql()
	})
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
