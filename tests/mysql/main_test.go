package mysql

import (
	"context"
	"database/sql"
	jetmysql "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	"github.com/stretchr/testify/require"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/pkg/profile"
	"os"
	"testing"
)

var db *sql.DB

var source string

const MariaDB = "MariaDB"

func init() {
	source = os.Getenv("MY_SQL_SOURCE")
}

func sourceIsMariaDB() bool {
	return source == MariaDB
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().Unix())
	defer profile.Start().Stop()

	var err error
	db, err = sql.Open("mysql", dbconfig.MySQLConnectionString)
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

func init() {
	jetmysql.SetLogger(func(ctx context.Context, statement jetmysql.PrintableStatement) {
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
