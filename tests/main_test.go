package tests

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/pkg/profile"
	"github.com/sub0zero/go-sqlbuilder/tests/.test_files/dvd_rental/dvds/model"
	"gotest.tools/assert"
	"os"
	"reflect"
	"testing"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "dvd_rental"
)

var connectString = fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
var db *sql.DB

//var tx *sql.Tx

//go:generate generator -host=localhost -port=5432 -user=postgres -password=postgres -dbname=dvd_rental -schema dvds -path .test_files
//go:generate generator -host=localhost -port=5432 -user=postgres -password=postgres -dbname=dvd_rental -sslmode=disable -schema test_sample -path .test_files

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

func TestGenerateModel(t *testing.T) {

	actor := model.Actor{}

	assert.Equal(t, reflect.TypeOf(actor.ActorID).String(), "int32")
	actorIDField, ok := reflect.TypeOf(actor).FieldByName("ActorID")
	assert.Assert(t, ok)
	assert.Equal(t, actorIDField.Tag.Get("sql"), "unique")
	assert.Equal(t, reflect.TypeOf(actor.FirstName).String(), "string")
	assert.Equal(t, reflect.TypeOf(actor.LastName).String(), "string")
	assert.Equal(t, reflect.TypeOf(actor.LastUpdate).String(), "time.Time")

	filmActor := model.FilmActor{}

	assert.Equal(t, reflect.TypeOf(filmActor.FilmID).String(), "int16")
	filmIDField, ok := reflect.TypeOf(filmActor).FieldByName("FilmID")
	assert.Assert(t, ok)
	assert.Equal(t, filmIDField.Tag.Get("sql"), "unique")

	assert.Equal(t, reflect.TypeOf(filmActor.ActorID).String(), "int16")
	actorIDField, ok = reflect.TypeOf(filmActor).FieldByName("ActorID")
	assert.Assert(t, ok)
	assert.Equal(t, filmIDField.Tag.Get("sql"), "unique")

	staff := model.Staff{}

	assert.Equal(t, reflect.TypeOf(staff.Email).String(), "*string")
	assert.Equal(t, reflect.TypeOf(staff.Picture).String(), "*[]uint8")
}
