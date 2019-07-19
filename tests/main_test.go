package tests

import (
	"bytes"
	"database/sql"
	"github.com/go-jet/jet/generator/postgres"
	"github.com/go-jet/jet/tests/.gentestdata/jetdb/dvds/model"
	"github.com/go-jet/jet/tests/dbconfig"
	_ "github.com/lib/pq"
	"github.com/pkg/profile"
	"gotest.tools/assert"
	"io/ioutil"
	"os"
	"os/exec"
	"reflect"
	"testing"
)

var db *sql.DB

func TestMain(m *testing.M) {
	defer profile.Start().Stop()

	var err error
	db, err = sql.Open("postgres", dbconfig.ConnectString)
	if err != nil {
		panic("Failed to connect to test db")
	}
	defer db.Close()

	ret := m.Run()

	os.Exit(ret)
}

func TestGeneratedModel(t *testing.T) {

	actor := model.Actor{}

	assert.Equal(t, reflect.TypeOf(actor.ActorID).String(), "int32")
	actorIDField, ok := reflect.TypeOf(actor).FieldByName("ActorID")
	assert.Assert(t, ok)
	assert.Equal(t, actorIDField.Tag.Get("sql"), "primary_key")
	assert.Equal(t, reflect.TypeOf(actor.FirstName).String(), "string")
	assert.Equal(t, reflect.TypeOf(actor.LastName).String(), "string")
	assert.Equal(t, reflect.TypeOf(actor.LastUpdate).String(), "time.Time")

	filmActor := model.FilmActor{}

	assert.Equal(t, reflect.TypeOf(filmActor.FilmID).String(), "int16")
	filmIDField, ok := reflect.TypeOf(filmActor).FieldByName("FilmID")
	assert.Assert(t, ok)
	assert.Equal(t, filmIDField.Tag.Get("sql"), "primary_key")

	assert.Equal(t, reflect.TypeOf(filmActor.ActorID).String(), "int16")
	actorIDField, ok = reflect.TypeOf(filmActor).FieldByName("ActorID")
	assert.Assert(t, ok)
	assert.Equal(t, filmIDField.Tag.Get("sql"), "primary_key")

	staff := model.Staff{}

	assert.Equal(t, reflect.TypeOf(staff.Email).String(), "*string")
	assert.Equal(t, reflect.TypeOf(staff.Picture).String(), "*[]uint8")
}

const genTestDir2 = "./.gentestdata2"

func TestCmdGenerator(t *testing.T) {
	err := os.RemoveAll(genTestDir2)
	assert.NilError(t, err)

	cmd := exec.Command("jet", "-dbname=jetdb", "-host=localhost", "-port=5432",
		"-user=jet", "-password=jet", "-schema=dvds", "-path="+genTestDir2)

	err = cmd.Run()
	assert.NilError(t, err)

	assertGeneratedFiles(t)

	err = os.RemoveAll(genTestDir2)
	assert.NilError(t, err)
}

func TestGenerator(t *testing.T) {

	err := os.RemoveAll(genTestDir2)
	assert.NilError(t, err)

	err = postgres.Generate(genTestDir2, postgres.DBConnection{
		Host:     dbconfig.Host,
		Port:     dbconfig.Port,
		User:     dbconfig.User,
		Password: dbconfig.Password,
		SslMode:  "disable",
		Params:   "",

		DBName:     dbconfig.DBName,
		SchemaName: "dvds",
	})

	assert.NilError(t, err)

	assertGeneratedFiles(t)

	err = os.RemoveAll(genTestDir2)
	assert.NilError(t, err)
}

func assertGeneratedFiles(t *testing.T) {
	// Table SQL Builder files
	tableSQLBuilderFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/table")
	assert.NilError(t, err)

	assertFileNameEqual(t, tableSQLBuilderFiles, "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go")

	assertFileContent(t, "./.gentestdata2/jetdb/dvds/table/actor.go", "\npackage table", actorSQLBuilderFile)

	// Enums SQL Builder files
	enumFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/enum")

	assertFileNameEqual(t, enumFiles, "mpaa_rating.go")
	assertFileContent(t, "./.gentestdata2/jetdb/dvds/enum/mpaa_rating.go", "\npackage enum", mpaaRatingEnumFile)

	// Model files
	modelFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/model")
	assert.NilError(t, err)

	assertFileNameEqual(t, modelFiles, "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go", "mpaa_rating.go")

	assertFileContent(t, "./.gentestdata2/jetdb/dvds/model/actor.go", "\npackage model", actorModelFile)
}

func assertFileContent(t *testing.T, filePath string, contentBegin string, expectedContent string) {
	enumFileData, err := ioutil.ReadFile(filePath)

	assert.NilError(t, err)

	beginIndex := bytes.Index(enumFileData, []byte(contentBegin))

	//fmt.Println("-"+string(enumFileData[beginIndex:])+"-")

	assert.DeepEqual(t, string(enumFileData[beginIndex:]), expectedContent)
}

func assertFileNameEqual(t *testing.T, fileInfos []os.FileInfo, fileNames ...string) {

	fileNamesMap := map[string]bool{}

	for _, fileInfo := range fileInfos {
		fileNamesMap[fileInfo.Name()] = true
	}

	for _, fileName := range fileNames {
		assert.Assert(t, fileNamesMap[fileName], fileName+" does not exist.")
	}
}

var mpaaRatingEnumFile = `
package enum

import "github.com/go-jet/jet"

var MpaaRating = &struct {
	G    jet.StringExpression
	Pg   jet.StringExpression
	Pg13 jet.StringExpression
	R    jet.StringExpression
	Nc17 jet.StringExpression
}{
	G:    jet.NewEnumValue("G"),
	Pg:   jet.NewEnumValue("PG"),
	Pg13: jet.NewEnumValue("PG-13"),
	R:    jet.NewEnumValue("R"),
	Nc17: jet.NewEnumValue("NC-17"),
}
`

var actorSQLBuilderFile = `
package table

import (
	"github.com/go-jet/jet"
)

var Actor = newActorTable()

type ActorTable struct {
	jet.Table

	//Columns
	ActorID    jet.ColumnInteger
	FirstName  jet.ColumnString
	LastName   jet.ColumnString
	LastUpdate jet.ColumnTimestamp

	AllColumns     jet.ColumnList
	MutableColumns jet.ColumnList
}

// creates new ActorTable with assigned alias
func (a *ActorTable) AS(alias string) *ActorTable {
	aliasTable := newActorTable()

	aliasTable.Table.AS(alias)

	return aliasTable
}

func newActorTable() *ActorTable {
	var (
		ActorIDColumn    = jet.IntegerColumn("actor_id")
		FirstNameColumn  = jet.StringColumn("first_name")
		LastNameColumn   = jet.StringColumn("last_name")
		LastUpdateColumn = jet.TimestampColumn("last_update")
	)

	return &ActorTable{
		Table: jet.NewTable("dvds", "actor", ActorIDColumn, FirstNameColumn, LastNameColumn, LastUpdateColumn),

		//Columns
		ActorID:    ActorIDColumn,
		FirstName:  FirstNameColumn,
		LastName:   LastNameColumn,
		LastUpdate: LastUpdateColumn,

		AllColumns:     jet.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, LastUpdateColumn},
		MutableColumns: jet.ColumnList{FirstNameColumn, LastNameColumn, LastUpdateColumn},
	}
}
`

var actorModelFile = `
package model

import (
	"time"
)

type Actor struct {
	ActorID    int32 ` + "`sql:\"primary_key\"`" + `
	FirstName  string
	LastName   string
	LastUpdate time.Time
}
`
