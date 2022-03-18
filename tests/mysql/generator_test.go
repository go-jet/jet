package mysql

import (
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/go-jet/jet/v2/generator/mysql"
	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	"github.com/stretchr/testify/require"
)

const genTestDirRoot = "./.gentestdata3"
const genTestDir3 = "./.gentestdata3/mysql"

func TestGenerator(t *testing.T) {

	for i := 0; i < 3; i++ {
		err := mysql.Generate(genTestDir3, dbConnection("dvds"))

		require.NoError(t, err)

		assertGeneratedFiles(t)
	}

	for i := 0; i < 3; i++ {
		dsn := dbconfig.MySQLConnectionString(sourceIsMariaDB(), "dvds")

		err := mysql.GenerateDSN(dsn, genTestDir3)

		require.NoError(t, err)
		assertGeneratedFiles(t)
	}

	err := os.RemoveAll(genTestDirRoot)
	require.NoError(t, err)
}

func TestCmdGenerator(t *testing.T) {
	err := os.RemoveAll(genTestDir3)
	require.NoError(t, err)

	var cmd *exec.Cmd

	if sourceIsMariaDB() {
		cmd = exec.Command("jet",
			"-source=MariaDB",
			"-dbname=dvds",
			"-host="+dbconfig.MariaDBHost,
			"-port="+strconv.Itoa(dbconfig.MariaDBPort),
			"-user="+dbconfig.MariaDBUser,
			"-password="+dbconfig.MariaDBPassword,
			"-path="+genTestDir3)
	} else {
		cmd = exec.Command("jet",
			"-source=MySQL",
			"-dbname=dvds",
			"-host="+dbconfig.MySqLHost,
			"-port="+strconv.Itoa(dbconfig.MySQLPort),
			"-user="+dbconfig.MySQLUser,
			"-password="+dbconfig.MySQLPassword,
			"-path="+genTestDir3)
	}

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Run()
	require.NoError(t, err)

	assertGeneratedFiles(t)

	err = os.RemoveAll(genTestDirRoot)
	require.NoError(t, err)

	// check that generation via DSN works
	dsn := "mysql://" + dbconfig.MySQLConnectionString(sourceIsMariaDB(), "dvds")
	cmd = exec.Command("jet", "-dsn="+dsn, "-path="+genTestDir3)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Run()
	require.NoError(t, err)
}

func TestIgnoreTablesViewsEnums(t *testing.T) {
	cmd := exec.Command("jet",
		"-source=MySQL",
		"-dbname=dvds",
		"-host="+dbconfig.MySqLHost,
		"-port="+strconv.Itoa(dbconfig.MySQLPort),
		"-user="+dbconfig.MySQLUser,
		"-password="+dbconfig.MySQLPassword,
		"-ignore-tables=actor,ADDRESS,Category, city ,country,staff,store,rental",
		"-ignore-views=actor_info,CUSTomER_LIST, film_list",
		"-ignore-enums=film_list_rating,film_rating",
		"-path="+genTestDir3)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err := cmd.Run()
	require.NoError(t, err)

	tableSQLBuilderFiles, err := ioutil.ReadDir(genTestDir3 + "/dvds/table")
	require.NoError(t, err)
	testutils.AssertFileNamesEqual(t, tableSQLBuilderFiles, "customer.go", "film.go", "film_actor.go",
		"film_category.go", "film_text.go", "inventory.go", "language.go", "payment.go")

	viewSQLBuilderFiles, err := ioutil.ReadDir(genTestDir3 + "/dvds/view")
	require.NoError(t, err)
	testutils.AssertFileNamesEqual(t, viewSQLBuilderFiles, "nicer_but_slower_film_list.go",
		"sales_by_film_category.go", "sales_by_store.go", "staff_list.go")

	enumFiles, err := ioutil.ReadDir(genTestDir3 + "/dvds/enum")
	require.NoError(t, err)
	testutils.AssertFileNamesEqual(t, enumFiles, "nicer_but_slower_film_list_rating.go")

	modelFiles, err := ioutil.ReadDir(genTestDir3 + "/dvds/model")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, modelFiles,
		"customer.go", "film.go", "film_actor.go", "film_category.go", "film_text.go", "inventory.go", "language.go",
		"payment.go", "nicer_but_slower_film_list_rating.go", "nicer_but_slower_film_list.go", "sales_by_film_category.go",
		"sales_by_store.go", "staff_list.go")
}

func assertGeneratedFiles(t *testing.T) {
	// Table SQL Builder files
	tableSQLBuilderFiles, err := ioutil.ReadDir(genTestDir3 + "/dvds/table")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, tableSQLBuilderFiles, "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "film_text.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go")

	testutils.AssertFileContent(t, genTestDir3+"/dvds/table/actor.go", actorSQLBuilderFile)

	// View SQL Builder files
	viewSQLBuilderFiles, err := ioutil.ReadDir(genTestDir3 + "/dvds/view")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, viewSQLBuilderFiles, "actor_info.go", "film_list.go", "nicer_but_slower_film_list.go",
		"sales_by_film_category.go", "customer_list.go", "sales_by_store.go", "staff_list.go")

	testutils.AssertFileContent(t, genTestDir3+"/dvds/view/actor_info.go", actorInfoSQLBuilderFile)

	// Enums SQL Builder files
	enumFiles, err := ioutil.ReadDir(genTestDir3 + "/dvds/enum")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, enumFiles, "film_rating.go", "film_list_rating.go", "nicer_but_slower_film_list_rating.go")
	testutils.AssertFileContent(t, genTestDir3+"/dvds/enum/film_rating.go", mpaaRatingEnumFile)

	// Model files
	modelFiles, err := ioutil.ReadDir(genTestDir3 + "/dvds/model")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, modelFiles, "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "film_text.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go",
		"film_rating.go", "film_list_rating.go", "nicer_but_slower_film_list_rating.go",
		"actor_info.go", "film_list.go", "nicer_but_slower_film_list.go", "sales_by_film_category.go",
		"customer_list.go", "sales_by_store.go", "staff_list.go")

	testutils.AssertFileContent(t, genTestDir3+"/dvds/model/actor.go", actorModelFile)
}

var mpaaRatingEnumFile = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package enum

import "github.com/go-jet/jet/v2/mysql"

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
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package table

import (
	"github.com/go-jet/jet/v2/mysql"
)

var Actor = newActorTable("dvds", "actor", "")

type ActorTable struct {
	mysql.Table

	//Columns
	ActorID    mysql.ColumnInteger
	FirstName  mysql.ColumnString
	LastName   mysql.ColumnString
	LastUpdate mysql.ColumnTimestamp

	AllColumns     mysql.ColumnList
	MutableColumns mysql.ColumnList
}

// AS creates new ActorTable with assigned alias
func (a ActorTable) AS(alias string) ActorTable {
	return newActorTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new ActorTable with assigned schema name
func (a ActorTable) FromSchema(schemaName string) ActorTable {
	return newActorTable(schemaName, a.TableName(), a.Alias())
}

// WithPrefix creates new ActorTable with assigned table prefix
func (a ActorTable) WithPrefix(prefix string) ActorTable {
	return newActorTable(a.SchemaName(), prefix+a.TableName(), a.TableName())
}

// WithSuffix creates new ActorTable with assigned table suffix
func (a ActorTable) WithSuffix(suffix string) ActorTable {
	return newActorTable(a.SchemaName(), a.TableName()+suffix, a.TableName())
}

func newActorTable(schemaName, tableName, alias string) ActorTable {
	var (
		ActorIDColumn    = mysql.IntegerColumn("actor_id")
		FirstNameColumn  = mysql.StringColumn("first_name")
		LastNameColumn   = mysql.StringColumn("last_name")
		LastUpdateColumn = mysql.TimestampColumn("last_update")
		allColumns       = mysql.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, LastUpdateColumn}
		mutableColumns   = mysql.ColumnList{FirstNameColumn, LastNameColumn, LastUpdateColumn}
	)

	return ActorTable{
		Table: mysql.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		ActorID:    ActorIDColumn,
		FirstName:  FirstNameColumn,
		LastName:   LastNameColumn,
		LastUpdate: LastUpdateColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
`

var actorModelFile = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

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

var actorInfoSQLBuilderFile = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package view

import (
	"github.com/go-jet/jet/v2/mysql"
)

var ActorInfo = newActorInfoTable("dvds", "actor_info", "")

type ActorInfoTable struct {
	mysql.Table

	//Columns
	ActorID   mysql.ColumnInteger
	FirstName mysql.ColumnString
	LastName  mysql.ColumnString
	FilmInfo  mysql.ColumnString

	AllColumns     mysql.ColumnList
	MutableColumns mysql.ColumnList
}

// AS creates new ActorInfoTable with assigned alias
func (a ActorInfoTable) AS(alias string) ActorInfoTable {
	return newActorInfoTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new ActorInfoTable with assigned schema name
func (a ActorInfoTable) FromSchema(schemaName string) ActorInfoTable {
	return newActorInfoTable(schemaName, a.TableName(), a.Alias())
}

// WithPrefix creates new ActorInfoTable with assigned table prefix
func (a ActorInfoTable) WithPrefix(prefix string) ActorInfoTable {
	return newActorInfoTable(a.SchemaName(), prefix+a.TableName(), a.TableName())
}

// WithSuffix creates new ActorInfoTable with assigned table suffix
func (a ActorInfoTable) WithSuffix(suffix string) ActorInfoTable {
	return newActorInfoTable(a.SchemaName(), a.TableName()+suffix, a.TableName())
}

func newActorInfoTable(schemaName, tableName, alias string) ActorInfoTable {
	var (
		ActorIDColumn   = mysql.IntegerColumn("actor_id")
		FirstNameColumn = mysql.StringColumn("first_name")
		LastNameColumn  = mysql.StringColumn("last_name")
		FilmInfoColumn  = mysql.StringColumn("film_info")
		allColumns      = mysql.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, FilmInfoColumn}
		mutableColumns  = mysql.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, FilmInfoColumn}
	)

	return ActorInfoTable{
		Table: mysql.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		ActorID:   ActorIDColumn,
		FirstName: FirstNameColumn,
		LastName:  LastNameColumn,
		FilmInfo:  FilmInfoColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
`
