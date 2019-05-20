package tests

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/pkg/profile"
	"github.com/sub0zero/go-sqlbuilder/generator"
	"gotest.tools/assert"
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

	defer profile.Start().Stop()

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
);

DROP TABLE IF EXISTS test_sample.employee;

CREATE TABLE test_sample.employee (
 employee_id INT PRIMARY KEY,
 first_name VARCHAR (255) NOT NULL,
 last_name VARCHAR (255) NOT NULL,
 manager_id INT,
 FOREIGN KEY (manager_id) 
 REFERENCES test_sample.employee (employee_id) 
 ON DELETE CASCADE
);
INSERT INTO test_sample.employee (
 employee_id,
 first_name,
 last_name,
 manager_id
)
VALUES
 (1, 'Windy', 'Hays', NULL),
 (2, 'Ava', 'Christensen', 1),
 (3, 'Hassan', 'Conner', 1),
 (4, 'Anna', 'Reeves', 2),
 (5, 'Sau', 'Norman', 2),
 (6, 'Kelsie', 'Hays', 3),
 (7, 'Tory', 'Goff', 3),
 (8, 'Salley', 'Lester', 3);

`

	result, err := db.Exec(linkTableCreate)

	if err != nil {
		panic(err)
	}

	fmt.Println(result)

}

func queryAll(t *testing.T, query string, args []interface{}) {
	rows, err := db.Query(query, args...)

	assert.NilError(t, err)

	defer rows.Close()

	for rows.Next() {
		//err := rows.Scan(scanContext.row...)
		//
		//assert.NilError(t, err)
	}

	err = rows.Err()

	assert.NilError(t, err)
}

func TestGenerateModel(t *testing.T) {

	err := generator.Generate(folderPath, connectString, dbname, schemaName)

	assert.NilError(t, err)

	//err = generator.Generate(folderPath, connectString, dbname, "sport")
	//
	//assert.NilError(t, err)
}
