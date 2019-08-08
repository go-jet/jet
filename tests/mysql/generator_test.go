package mysql

import (
	"bytes"
	"github.com/go-jet/jet/generator/mysql"
	"github.com/go-jet/jet/tests/dbconfig"
	"gotest.tools/assert"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
)

const genTestDir2 = "./.gentestdata2/mysql"

func TestGenerator(t *testing.T) {

	err := os.RemoveAll(genTestDir2)
	assert.NilError(t, err)

	err = mysql.Generate(genTestDir2, mysql.DBConnection{
		Host:     dbconfig.MySqLHost,
		Port:     dbconfig.MySQLPort,
		User:     dbconfig.MySQLUser,
		Password: dbconfig.MySQLPassword,
		DBName:   "dvds",
	})

	assert.NilError(t, err)

	assertGeneratedFiles(t)

	err = os.RemoveAll(genTestDir2)
	assert.NilError(t, err)
}

func TestCmdGenerator(t *testing.T) {
	goInstallJet := exec.Command("sh", "-c", "go install github.com/go-jet/jet/cmd/jet")
	goInstallJet.Stderr = os.Stderr
	err := goInstallJet.Run()
	assert.NilError(t, err)

	err = os.RemoveAll(genTestDir2)
	assert.NilError(t, err)

	cmd := exec.Command("jet", "-source=MySQL", "-dbname=dvds", "-host=localhost", "-port=3306",
		"-user=jet", "-password=jet", "-path="+genTestDir2)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Run()
	assert.NilError(t, err)

	assertGeneratedFiles(t)

	err = os.RemoveAll(genTestDir2)
	assert.NilError(t, err)
}

func assertGeneratedFiles(t *testing.T) {
	// Table SQL Builder files
	tableSQLBuilderFiles, err := ioutil.ReadDir("./.gentestdata2/mysql/dvds/table")
	assert.NilError(t, err)

	assertFileNameEqual(t, tableSQLBuilderFiles, "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go")

	assertFileContent(t, "./.gentestdata2/mysql/dvds/table/actor.go", "\npackage table", actorSQLBuilderFile)

	// Enums SQL Builder files
	enumFiles, err := ioutil.ReadDir("./.gentestdata2/mysql/dvds/enum")
	assert.NilError(t, err)

	assertFileNameEqual(t, enumFiles, "film_list_rating.go", "film_rating.go", "nicer_but_slower_film_list_rating.go")
	assertFileContent(t, "./.gentestdata2/mysql/dvds/enum/film_rating.go", "\npackage enum", mpaaRatingEnumFile)

	// Model files
	modelFiles, err := ioutil.ReadDir("./.gentestdata2/mysql/dvds/model")
	assert.NilError(t, err)

	assertFileNameEqual(t, modelFiles, "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go", "film_list_rating.go", "film_rating.go", "nicer_but_slower_film_list_rating.go")

	assertFileContent(t, "./.gentestdata2/mysql/dvds/model/actor.go", "\npackage model", actorModelFile)
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

import "github.com/go-jet/jet/mysql"

var FilmRating = &struct {
	G    mysql.StringExpression
	Pg   mysql.StringExpression
	Pg13 mysql.StringExpression
	R    mysql.StringExpression
	Nc17 mysql.StringExpression
}{
	G:    mysql.NewEnumValue("G"),
	Pg:   mysql.NewEnumValue("PG"),
	Pg13: mysql.NewEnumValue("PG-13"),
	R:    mysql.NewEnumValue("R"),
	Nc17: mysql.NewEnumValue("NC-17"),
}
`

var actorSQLBuilderFile = `
package table

import (
	"github.com/go-jet/jet/mysql"
)

var Actor = newActorTable()

type ActorTable struct {
	mysql.Table

	//Columns
	ActorID    mysql.ColumnInteger
	FirstName  mysql.ColumnString
	LastName   mysql.ColumnString
	LastUpdate mysql.ColumnTimestamp

	AllColumns     mysql.IColumnList
	MutableColumns mysql.IColumnList
}

// creates new ActorTable with assigned alias
func (a *ActorTable) AS(alias string) *ActorTable {
	aliasTable := newActorTable()

	aliasTable.Table.AS(alias)

	return aliasTable
}

func newActorTable() *ActorTable {
	var (
		ActorIDColumn    = mysql.IntegerColumn("actor_id")
		FirstNameColumn  = mysql.StringColumn("first_name")
		LastNameColumn   = mysql.StringColumn("last_name")
		LastUpdateColumn = mysql.TimestampColumn("last_update")
	)

	return &ActorTable{
		Table: mysql.NewTable("dvds", "actor", ActorIDColumn, FirstNameColumn, LastNameColumn, LastUpdateColumn),

		//Columns
		ActorID:    ActorIDColumn,
		FirstName:  FirstNameColumn,
		LastName:   LastNameColumn,
		LastUpdate: LastUpdateColumn,

		AllColumns:     mysql.ColumnList(ActorIDColumn, FirstNameColumn, LastNameColumn, LastUpdateColumn),
		MutableColumns: mysql.ColumnList(FirstNameColumn, LastNameColumn, LastUpdateColumn),
	}
}
`

var actorModelFile = `
package model

import (
	"time"
)

type Actor struct {
	ActorID    uint16 ` + "`sql:\"primary_key\"`" + `
	FirstName  string
	LastName   string
	LastUpdate time.Time
}
`
