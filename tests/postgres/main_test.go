package postgres

import (
	"database/sql"
	"github.com/go-jet/jet/tests/dbconfig"
	_ "github.com/lib/pq"
	"github.com/pkg/profile"
	"os"
	"testing"
)

var db *sql.DB

func TestMain(m *testing.M) {
	defer profile.Start().Stop()

	var err error
	db, err = sql.Open("postgres", dbconfig.PostgresConnectString)
	if err != nil {
		panic("Failed to connect to test db")
	}
	defer db.Close()

	ret := m.Run()

	os.Exit(ret)
}
