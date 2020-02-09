package postgres

import (
	"database/sql"
	"github.com/go-jet/jet/tests/dbconfig"
	_ "github.com/lib/pq"
	"github.com/pkg/profile"
	"os"
	"os/exec"
	"strings"
	"testing"
)

var db *sql.DB
var testRoot string

func TestMain(m *testing.M) {
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
