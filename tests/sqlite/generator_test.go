package sqlite

import (
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/sqlite"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/testutils"
	sqlite2 "github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/sakila/model"
	"github.com/go-jet/jet/v2/tests/internal/utils/repo"
)

func TestGeneratedModel(t *testing.T) {
	actor := model.Actor{}

	require.Equal(t, reflect.TypeOf(actor.ActorID).String(), "int32")
	actorIDField, ok := reflect.TypeOf(actor).FieldByName("ActorID")
	require.True(t, ok)
	require.Equal(t, actorIDField.Tag.Get("sql"), "primary_key")
	require.Equal(t, reflect.TypeOf(actor.FirstName).String(), "string")
	require.Equal(t, reflect.TypeOf(actor.LastName).String(), "string")
	require.Equal(t, reflect.TypeOf(actor.LastUpdate).String(), "time.Time")

	filmActor := model.FilmActor{}

	require.Equal(t, reflect.TypeOf(filmActor.FilmID).String(), "int32")
	filmIDField, ok := reflect.TypeOf(filmActor).FieldByName("FilmID")
	require.True(t, ok)
	require.Equal(t, filmIDField.Tag.Get("sql"), "primary_key")

	require.Equal(t, reflect.TypeOf(filmActor.ActorID).String(), "int32")
	actorIDField, ok = reflect.TypeOf(filmActor).FieldByName("ActorID")
	require.True(t, ok)
	require.Equal(t, filmIDField.Tag.Get("sql"), "primary_key")

	staff := model.Staff{}

	require.Equal(t, reflect.TypeOf(staff.Email).String(), "*string")
	require.Equal(t, reflect.TypeOf(staff.Picture).String(), "*[]uint8")
}

var testDatabaseFilePath = repo.GetTestDataFilePath("/init/sqlite/sakila.db")
var genDestDir = repo.GetTestsFilePath("/sqlite/.gen")

func TestGenerator(t *testing.T) {
	for i := 0; i < 3; i++ {
		err := sqlite.GenerateDSN(testDatabaseFilePath, genDestDir)
		require.NoError(t, err)

		assertGeneratedFiles(t)
	}

	err := os.RemoveAll(genDestDir)
	require.NoError(t, err)
}

func TestGenerator_TableMetadata(t *testing.T) {
	var schema metadata.Schema
	err := sqlite.GenerateDSN(testDatabaseFilePath, genDestDir,
		template.Default(sqlite2.Dialect).UseSchema(func(m metadata.Schema) template.Schema {
			schema = m
			return template.DefaultSchema(m)
		}))
	require.NoError(t, err)

	// Spot check the actor table and assert that the emitted
	// properties are as expected.
	var got metadata.Table
	for _, table := range schema.TablesMetaData {
		if table.Name == "actor" {
			got = table
		}
	}

	want := metadata.Table{
		Name: "actor",
		Columns: []metadata.Column{
			{Name: "actor_id", IsPrimaryKey: true, IsNullable: false, IsGenerated: false, HasDefault: false, DataType: metadata.DataType{Name: "INTEGER", Kind: "base", IsUnsigned: false}, Comment: ""},
			{Name: "first_name", IsPrimaryKey: false, IsNullable: false, IsGenerated: false, HasDefault: false, DataType: metadata.DataType{Name: "VARCHAR", Kind: "base", IsUnsigned: false}, Comment: ""},
			{Name: "last_name", IsPrimaryKey: false, IsNullable: false, IsGenerated: false, HasDefault: false, DataType: metadata.DataType{Name: "VARCHAR", Kind: "base", IsUnsigned: false}, Comment: ""},
			{Name: "last_update", IsPrimaryKey: false, IsNullable: false, IsGenerated: false, HasDefault: true, DataType: metadata.DataType{Name: "TIMESTAMP", Kind: "base", IsUnsigned: false}, Comment: ""},
		},
	}
	require.Equal(t, want, got)
}

func TestCmdGenerator(t *testing.T) {
	cmd := exec.Command("jet", "-source=SQLite", "-dsn=file://"+testDatabaseFilePath, "-path="+genDestDir)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err := cmd.Run()
	require.NoError(t, err)

	assertGeneratedFiles(t)

	err = os.RemoveAll(genDestDir)
	require.NoError(t, err)
}

func TestCmdGeneratorIgnoreTablesViewsEnums(t *testing.T) {
	cmd := exec.Command("jet",
		"-source=SQLite",
		"-dsn=file://"+testDatabaseFilePath,
		"-ignore-tables=actor,Address,CATEGORY , city ,film,rental,store",
		"-ignore-views=customer_list, film_list,STAFF_LIst",
		"-path="+genDestDir)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err := cmd.Run()
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, genDestDir+"/table", "country.go",
		"customer.go", "film_actor.go", "film_category.go", "film_text.go", "inventory.go", "language.go",
		"payment.go", "staff.go", "table_use_schema.go")

	testutils.AssertFileNamesEqual(t, genDestDir+"/view", "sales_by_film_category.go",
		"sales_by_store.go", "view_use_schema.go")

	testutils.AssertFileNamesEqual(t, genDestDir+"/model", "country.go",
		"customer.go", "film_actor.go", "film_category.go", "film_text.go", "inventory.go", "language.go",
		"payment.go", "staff.go", "sales_by_film_category.go", "sales_by_store.go")
}

func assertGeneratedFiles(t *testing.T) {
	// Table SQL Builder files
	testutils.AssertFileNamesEqual(t, genDestDir+"/table", "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "film_text.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go", "table_use_schema.go")

	testutils.AssertFileContent(t, genDestDir+"/table/actor.go", actorSQLBuilderFile)
	testutils.AssertFileContent(t, genDestDir+"/table/table_use_schema.go", tableUseSchemaFile)

	// View SQL Builder files
	testutils.AssertFileNamesEqual(t, genDestDir+"/view", "film_list.go", "sales_by_film_category.go",
		"customer_list.go", "sales_by_store.go", "staff_list.go", "view_use_schema.go")

	testutils.AssertFileContent(t, genDestDir+"/view/film_list.go", filmListSQLBuilderFile)
	testutils.AssertFileContent(t, genDestDir+"/view/view_use_schema.go", viewUseSchemaFile)

	// Model files
	testutils.AssertFileNamesEqual(t, genDestDir+"/model", "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "film_text.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go",
		"film_list.go", "sales_by_film_category.go",
		"customer_list.go", "sales_by_store.go", "staff_list.go")

	testutils.AssertFileContent(t, genDestDir+"/model/address.go", addressModelFile)
}

const actorSQLBuilderFile = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package table

import (
	"github.com/go-jet/jet/v2/sqlite"
)

var Actor = newActorTable("", "actor", "")

type actorTable struct {
	sqlite.Table

	// Columns
	ActorID    sqlite.ColumnInteger
	FirstName  sqlite.ColumnString
	LastName   sqlite.ColumnString
	LastUpdate sqlite.ColumnTimestamp

	AllColumns     sqlite.ColumnList
	MutableColumns sqlite.ColumnList
	DefaultColumns sqlite.ColumnList
}

type ActorTable struct {
	actorTable

	EXCLUDED actorTable
}

// AS creates new ActorTable with assigned alias
func (a ActorTable) AS(alias string) *ActorTable {
	return newActorTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new ActorTable with assigned schema name
func (a ActorTable) FromSchema(schemaName string) *ActorTable {
	return newActorTable(schemaName, a.TableName(), a.Alias())
}

// WithPrefix creates new ActorTable with assigned table prefix
func (a ActorTable) WithPrefix(prefix string) *ActorTable {
	return newActorTable(a.SchemaName(), prefix+a.TableName(), a.TableName())
}

// WithSuffix creates new ActorTable with assigned table suffix
func (a ActorTable) WithSuffix(suffix string) *ActorTable {
	return newActorTable(a.SchemaName(), a.TableName()+suffix, a.TableName())
}

func newActorTable(schemaName, tableName, alias string) *ActorTable {
	return &ActorTable{
		actorTable: newActorTableImpl(schemaName, tableName, alias),
		EXCLUDED:   newActorTableImpl("", "excluded", ""),
	}
}

func newActorTableImpl(schemaName, tableName, alias string) actorTable {
	var (
		ActorIDColumn    = sqlite.IntegerColumn("actor_id")
		FirstNameColumn  = sqlite.StringColumn("first_name")
		LastNameColumn   = sqlite.StringColumn("last_name")
		LastUpdateColumn = sqlite.TimestampColumn("last_update")
		allColumns       = sqlite.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, LastUpdateColumn}
		mutableColumns   = sqlite.ColumnList{FirstNameColumn, LastNameColumn, LastUpdateColumn}
		defaultColumns   = sqlite.ColumnList{LastUpdateColumn}
	)

	return actorTable{
		Table: sqlite.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		ActorID:    ActorIDColumn,
		FirstName:  FirstNameColumn,
		LastName:   LastNameColumn,
		LastUpdate: LastUpdateColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
		DefaultColumns: defaultColumns,
	}
}
`
const tableUseSchemaFile = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package table

// UseSchema sets a new schema name for all generated table SQL builder types. It is recommended to invoke
// this method only once at the beginning of the program.
func UseSchema(schema string) {
	Actor = Actor.FromSchema(schema)
	Address = Address.FromSchema(schema)
	Category = Category.FromSchema(schema)
	City = City.FromSchema(schema)
	Country = Country.FromSchema(schema)
	Customer = Customer.FromSchema(schema)
	Film = Film.FromSchema(schema)
	FilmActor = FilmActor.FromSchema(schema)
	FilmCategory = FilmCategory.FromSchema(schema)
	FilmText = FilmText.FromSchema(schema)
	Inventory = Inventory.FromSchema(schema)
	Language = Language.FromSchema(schema)
	Payment = Payment.FromSchema(schema)
	Rental = Rental.FromSchema(schema)
	Staff = Staff.FromSchema(schema)
	Store = Store.FromSchema(schema)
}
`

const filmListSQLBuilderFile = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package view

import (
	"github.com/go-jet/jet/v2/sqlite"
)

var FilmList = newFilmListTable("", "film_list", "")

type filmListTable struct {
	sqlite.Table

	// Columns
	Fid         sqlite.ColumnInteger
	Title       sqlite.ColumnString
	Description sqlite.ColumnString
	Category    sqlite.ColumnString
	Price       sqlite.ColumnFloat
	Length      sqlite.ColumnInteger
	Rating      sqlite.ColumnString
	Actors      sqlite.ColumnString

	AllColumns     sqlite.ColumnList
	MutableColumns sqlite.ColumnList
	DefaultColumns sqlite.ColumnList
}

type FilmListTable struct {
	filmListTable

	EXCLUDED filmListTable
}

// AS creates new FilmListTable with assigned alias
func (a FilmListTable) AS(alias string) *FilmListTable {
	return newFilmListTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new FilmListTable with assigned schema name
func (a FilmListTable) FromSchema(schemaName string) *FilmListTable {
	return newFilmListTable(schemaName, a.TableName(), a.Alias())
}

// WithPrefix creates new FilmListTable with assigned table prefix
func (a FilmListTable) WithPrefix(prefix string) *FilmListTable {
	return newFilmListTable(a.SchemaName(), prefix+a.TableName(), a.TableName())
}

// WithSuffix creates new FilmListTable with assigned table suffix
func (a FilmListTable) WithSuffix(suffix string) *FilmListTable {
	return newFilmListTable(a.SchemaName(), a.TableName()+suffix, a.TableName())
}

func newFilmListTable(schemaName, tableName, alias string) *FilmListTable {
	return &FilmListTable{
		filmListTable: newFilmListTableImpl(schemaName, tableName, alias),
		EXCLUDED:      newFilmListTableImpl("", "excluded", ""),
	}
}

func newFilmListTableImpl(schemaName, tableName, alias string) filmListTable {
	var (
		FidColumn         = sqlite.IntegerColumn("FID")
		TitleColumn       = sqlite.StringColumn("title")
		DescriptionColumn = sqlite.StringColumn("description")
		CategoryColumn    = sqlite.StringColumn("category")
		PriceColumn       = sqlite.FloatColumn("price")
		LengthColumn      = sqlite.IntegerColumn("length")
		RatingColumn      = sqlite.StringColumn("rating")
		ActorsColumn      = sqlite.StringColumn("actors")
		allColumns        = sqlite.ColumnList{FidColumn, TitleColumn, DescriptionColumn, CategoryColumn, PriceColumn, LengthColumn, RatingColumn, ActorsColumn}
		mutableColumns    = sqlite.ColumnList{FidColumn, TitleColumn, DescriptionColumn, CategoryColumn, PriceColumn, LengthColumn, RatingColumn, ActorsColumn}
		defaultColumns    = sqlite.ColumnList{}
	)

	return filmListTable{
		Table: sqlite.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		Fid:         FidColumn,
		Title:       TitleColumn,
		Description: DescriptionColumn,
		Category:    CategoryColumn,
		Price:       PriceColumn,
		Length:      LengthColumn,
		Rating:      RatingColumn,
		Actors:      ActorsColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
		DefaultColumns: defaultColumns,
	}
}
`

const viewUseSchemaFile = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package view

// UseSchema sets a new schema name for all generated view SQL builder types. It is recommended to invoke
// this method only once at the beginning of the program.
func UseSchema(schema string) {
	CustomerList = CustomerList.FromSchema(schema)
	FilmList = FilmList.FromSchema(schema)
	SalesByFilmCategory = SalesByFilmCategory.FromSchema(schema)
	SalesByStore = SalesByStore.FromSchema(schema)
	StaffList = StaffList.FromSchema(schema)
}
`

const addressModelFile = `
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

type Address struct {
	AddressID  int32 ` + "`sql:\"primary_key\"`" + `
	Address    string
	Address2   *string
	District   string
	CityID     int32
	PostalCode *string
	Phone      string
	LastUpdate time.Time
}
`
