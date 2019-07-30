package mysql

import (
	"database/sql"
	"github.com/go-jet/jet/tests/dbconfig"

	_ "github.com/go-sql-driver/mysql"

	"github.com/pkg/profile"
	"os"
	"testing"
)

var db *sql.DB

func TestMain(m *testing.M) {
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
