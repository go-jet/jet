package postgres

import (
	"context"
	"database/sql"
	"github.com/go-jet/jet/postgres"
	"github.com/go-jet/jet/tests/dbconfig"
	_ "github.com/lib/pq"
	"github.com/pkg/profile"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

var db *sql.DB
var testRoot string

func TestMain(m *testing.M) {
	rand.Seed(time.Now().Unix())
	defer profile.Start().Stop()

	setTestRoot()

	var err error
	db, err = sql.Open("postgres", dbconfig.PostgresConnectString)
	if err != nil {
		panic("Failed to connect to test db")
	}
	defer db.Close()

	ret := m.Run()

	os.Exit(ret)
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
	postgres.SetLogger(func(ctx context.Context, statement postgres.LoggableStatement) {
		loggedSQL, loggedSQLArgs = statement.Sql()
		loggedDebugSQL = statement.DebugSql()
	})
}

func requireLogged(t *testing.T, statement postgres.Statement) {
	query, args := statement.Sql()
	require.Equal(t, loggedSQL, query)
	require.Equal(t, loggedSQLArgs, args)
	require.Equal(t, loggedDebugSQL, statement.DebugSql())
}
