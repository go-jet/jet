package postgres

import (
	"github.com/go-jet/jet/generator/postgres"
	"github.com/go-jet/jet/internal/testutils"
	"github.com/go-jet/jet/tests/dbconfig"
	"gotest.tools/assert"
	"io/ioutil"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/go-jet/jet/tests/.gentestdata/jetdb/dvds/model"
)

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
	goInstallJet := exec.Command("sh", "-c", "go install github.com/go-jet/jet/cmd/jet")
	goInstallJet.Stderr = os.Stderr
	err := goInstallJet.Run()
	assert.NilError(t, err)

	err = os.RemoveAll(genTestDir2)
	assert.NilError(t, err)

	cmd := exec.Command("jet", "-source=PostgreSQL", "-dbname=jetdb", "-host=localhost", "-port=5432",
		"-user=jet", "-password=jet", "-schema=dvds", "-path="+genTestDir2)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Run()
	assert.NilError(t, err)

	assertGeneratedFiles(t)

	err = os.RemoveAll(genTestDir2)
	assert.NilError(t, err)
}

func TestGenerator(t *testing.T) {

	for i := 0; i < 3; i++ {
		err := postgres.Generate(genTestDir2, postgres.DBConnection{
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
	}

	err := os.RemoveAll(genTestDir2)
	assert.NilError(t, err)
}

func assertGeneratedFiles(t *testing.T) {
	// Table SQL Builder files
	tableSQLBuilderFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/table")
	assert.NilError(t, err)

	testutils.AssertFileNamesEqual(t, tableSQLBuilderFiles, "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go")

	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/table/actor.go", "\npackage table", actorSQLBuilderFile)

	// View SQL Builder files
	viewSQLBuilderFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/view")
	assert.NilError(t, err)

	testutils.AssertFileNamesEqual(t, viewSQLBuilderFiles, "actor_info.go", "film_list.go", "nicer_but_slower_film_list.go",
		"sales_by_film_category.go", "customer_list.go", "sales_by_store.go", "staff_list.go")

	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/view/actor_info.go", "\npackage view", actorInfoSQLBuilderFile)

	// Enums SQL Builder files
	enumFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/enum")
	assert.NilError(t, err)

	testutils.AssertFileNamesEqual(t, enumFiles, "mpaa_rating.go")
	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/enum/mpaa_rating.go", "\npackage enum", mpaaRatingEnumFile)

	// Model files
	modelFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/model")
	assert.NilError(t, err)

	testutils.AssertFileNamesEqual(t, modelFiles, "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go", "mpaa_rating.go",
		"actor_info.go", "film_list.go", "nicer_but_slower_film_list.go", "sales_by_film_category.go",
		"customer_list.go", "sales_by_store.go", "staff_list.go")

	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/model/actor.go", "\npackage model", actorModelFile)
}

var mpaaRatingEnumFile = `
package enum

import "github.com/go-jet/jet/postgres"

var MpaaRating = &struct {
	G    postgres.StringExpression
	Pg   postgres.StringExpression
	Pg13 postgres.StringExpression
	R    postgres.StringExpression
	Nc17 postgres.StringExpression
}{
	G:    postgres.NewEnumValue("G"),
	Pg:   postgres.NewEnumValue("PG"),
	Pg13: postgres.NewEnumValue("PG-13"),
	R:    postgres.NewEnumValue("R"),
	Nc17: postgres.NewEnumValue("NC-17"),
}
`

var actorSQLBuilderFile = `
package table

import (
	"github.com/go-jet/jet/postgres"
)

var Actor = newActorTable()

type ActorTable struct {
	postgres.Table

	//Columns
	ActorID    postgres.ColumnInteger
	FirstName  postgres.ColumnString
	LastName   postgres.ColumnString
	LastUpdate postgres.ColumnTimestamp

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

// creates new ActorTable with assigned alias
func (a *ActorTable) AS(alias string) *ActorTable {
	aliasTable := newActorTable()

	aliasTable.Table.AS(alias)

	return aliasTable
}

func newActorTable() *ActorTable {
	var (
		ActorIDColumn    = postgres.IntegerColumn("actor_id")
		FirstNameColumn  = postgres.StringColumn("first_name")
		LastNameColumn   = postgres.StringColumn("last_name")
		LastUpdateColumn = postgres.TimestampColumn("last_update")
	)

	return &ActorTable{
		Table: postgres.NewTable("dvds", "actor", ActorIDColumn, FirstNameColumn, LastNameColumn, LastUpdateColumn),

		//Columns
		ActorID:    ActorIDColumn,
		FirstName:  FirstNameColumn,
		LastName:   LastNameColumn,
		LastUpdate: LastUpdateColumn,

		AllColumns:     postgres.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, LastUpdateColumn},
		MutableColumns: postgres.ColumnList{FirstNameColumn, LastNameColumn, LastUpdateColumn},
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

var actorInfoSQLBuilderFile = `
package view

import (
	"github.com/go-jet/jet/postgres"
)

var ActorInfo = newActorInfoTable()

type ActorInfoTable struct {
	postgres.Table

	//Columns
	ActorID   postgres.ColumnInteger
	FirstName postgres.ColumnString
	LastName  postgres.ColumnString
	FilmInfo  postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

// creates new ActorInfoTable with assigned alias
func (a *ActorInfoTable) AS(alias string) *ActorInfoTable {
	aliasTable := newActorInfoTable()

	aliasTable.Table.AS(alias)

	return aliasTable
}

func newActorInfoTable() *ActorInfoTable {
	var (
		ActorIDColumn   = postgres.IntegerColumn("actor_id")
		FirstNameColumn = postgres.StringColumn("first_name")
		LastNameColumn  = postgres.StringColumn("last_name")
		FilmInfoColumn  = postgres.StringColumn("film_info")
	)

	return &ActorInfoTable{
		Table: postgres.NewTable("dvds", "actor_info", ActorIDColumn, FirstNameColumn, LastNameColumn, FilmInfoColumn),

		//Columns
		ActorID:   ActorIDColumn,
		FirstName: FirstNameColumn,
		LastName:  LastNameColumn,
		FilmInfo:  FilmInfoColumn,

		AllColumns:     postgres.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, FilmInfoColumn},
		MutableColumns: postgres.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, FilmInfoColumn},
	}
}
`
