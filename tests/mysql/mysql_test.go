package mysql

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/tests/dbconfig"

	//_ "github.com/go-sql-driver/mysql"
	_ "github.com/ziutek/mymysql/godrv"

	"github.com/pkg/profile"
	"os"
	"testing"
)

var db *sql.DB

func TestMain(m *testing.M) {
	defer profile.Start().Stop()

	fmt.Println(dbconfig.MySQLConnectionString)

	var err error
	db, err = sql.Open("mysql", "jet:jet@tcp(localhost:3306)/")
	if err != nil {
		panic("Failed to connect to test db" + err.Error())
	}
	defer db.Close()

	ret := m.Run()

	os.Exit(ret)
}
