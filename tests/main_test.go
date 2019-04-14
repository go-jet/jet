package tests

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"os"
	"testing"
)

const (
	folderPath = ".test_files/"
	host       = "localhost"
	port       = 5432
	user       = "postgres"
	password   = "postgres"
	dbname     = "dvd_rental"
	schemaName = "dvds"
)

var connectString = fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
var db *sql.DB

//var tx *sql.Tx

//go:generate generator -db "host=localhost port=5432 user=postgres password=postgres dbname=dvd_rental sslmode=disable" -dbName dvd_rental -schema dvds -path .test_files
//go:generate generator -db "host=localhost port=5432 user=postgres password=postgres dbname=dvd_rental sslmode=disable" -dbName dvd_rental -schema test_sample -path .test_files

func TestMain(m *testing.M) {
	fmt.Println("Begin")

	var err error
	db, err = sql.Open("postgres", connectString)
	if err != nil {
		panic("Failed to connect to test db")
	}
	//tx, _ = db.Begin()
	defer cleanUp()

	dbInit()

	ret := m.Run()

	cleanUp()
	fmt.Println("END")

	os.Exit(ret)
}

func cleanUp() {
	fmt.Println("CLEAN UP")

	//tx.Rollback()
	db.Close()
}

func dbInit() {
	linkTableCreate := `
DROP TABLE IF EXISTS test_sample.link;

CREATE TABLE IF NOT EXISTS test_sample.link (
 ID serial PRIMARY KEY,
 url VARCHAR (255) NOT NULL,
 name VARCHAR (255) NOT NULL,
 description VARCHAR (255),
 rel VARCHAR (50)
);`

	result, err := db.Exec(linkTableCreate)

	if err != nil {
		panic(err)
	}

	fmt.Println(result)

}
