package mysql

import (
	"github.com/go-jet/jet/v2/tests/internal/utils/common"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/mysql"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/testutils"
	mysql2 "github.com/go-jet/jet/v2/mysql"
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
		dsn := ConnectionString(sourceIsMariaDB(), "dvds")

		err := mysql.GenerateDSN(dsn, genTestDir3)

		require.NoError(t, err)
		assertGeneratedFiles(t)
	}

	err := os.RemoveAll(genTestDirRoot)
	require.NoError(t, err)
}

func TestGenerator_TableMetadata(t *testing.T) {
	var schema metadata.Schema
	err := mysql.Generate(genTestDir3, dbConnection("dvds"),
		template.Default(mysql2.Dialect).UseSchema(func(m metadata.Schema) template.Schema {
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
			{Name: "actor_id", IsPrimaryKey: true, IsNullable: false, IsGenerated: false, HasDefault: false, DataType: metadata.DataType{Name: "smallint", Kind: "base", IsUnsigned: true}, Comment: ""},
			{Name: "first_name", IsPrimaryKey: false, IsNullable: false, IsGenerated: false, HasDefault: false, DataType: metadata.DataType{Name: "varchar", Kind: "base", IsUnsigned: false}, Comment: ""},
			{Name: "last_name", IsPrimaryKey: false, IsNullable: false, IsGenerated: false, HasDefault: false, DataType: metadata.DataType{Name: "varchar", Kind: "base", IsUnsigned: false}, Comment: ""},
			{Name: "last_update", IsPrimaryKey: false, IsNullable: false, IsGenerated: false, HasDefault: true, DataType: metadata.DataType{Name: "timestamp", Kind: "base", IsUnsigned: false}, Comment: ""},
		},
	}
	require.Equal(t, want, got)

	err = os.RemoveAll(genTestDirRoot)
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
			"-host="+MariaDBHost,
			"-port="+strconv.Itoa(MariaDBPort),
			"-user="+MariaDBUser,
			"-password="+MariaDBPassword,
			"-path="+genTestDir3)
	} else {
		cmd = exec.Command("jet",
			"-source=MySQL",
			"-dbname=dvds",
			"-host="+MySqLHost,
			"-port="+strconv.Itoa(MySQLPort),
			"-user="+MySQLUser,
			"-password="+MySQLPassword,
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
	dsn := "mysql://" + ConnectionString(sourceIsMariaDB(), "dvds")
	cmd = exec.Command("jet", "-dsn="+dsn, "-path="+genTestDir3)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Run()
	require.NoError(t, err)
}

func TestIgnoreTablesViewsEnums(t *testing.T) {
	var (
		host string
		port int
	)
	if sourceIsMariaDB() {
		host = MariaDBHost
		port = MariaDBPort
	} else {
		host = MySqLHost
		port = MySQLPort
	}
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "with dsn",
			args: []string{
				"-dsn=mysql://" + ConnectionString(sourceIsMariaDB(), "dvds"),
				"-ignore-tables=actor,ADDRESS,Category, city ,country,staff,store,rental",
				"-ignore-views=actor_info,CUSTomER_LIST, film_list",
				"-ignore-enums=film_list_rating,film_rating",
				"-path=" + genTestDir3,
			},
		},
		{
			name: "without dsn",
			args: []string{
				"-source=MySQL",
				"-dbname=dvds",
				"-host=" + host,
				"-port=" + strconv.Itoa(port),
				"-user=" + MySQLUser,
				"-password=" + MySQLPassword,
				"-ignore-tables=actor,ADDRESS,Category, city ,country,staff,store,rental",
				"-ignore-views=actor_info,CUSTomER_LIST, film_list",
				"-ignore-enums=film_list_rating,film_rating",
				"-path=" + genTestDir3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command("jet", tt.args...)

			cmd.Stderr = os.Stderr
			cmd.Stdout = os.Stdout

			err := cmd.Run()
			require.NoError(t, err)

			testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/table", "customer.go", "film.go", "film_actor.go",
				"film_category.go", "film_text.go", "inventory.go", "language.go", "payment.go", "table_use_schema.go")

			testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/view", "nicer_but_slower_film_list.go",
				"sales_by_film_category.go", "sales_by_store.go", "staff_list.go", "view_use_schema.go")

			testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/enum", "nicer_but_slower_film_list_rating.go")

			testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/model",
				"customer.go", "film.go", "film_actor.go", "film_category.go", "film_text.go", "inventory.go", "language.go",
				"payment.go", "nicer_but_slower_film_list_rating.go", "nicer_but_slower_film_list.go", "sales_by_film_category.go",
				"sales_by_store.go", "staff_list.go")
		})
	}
}

func assertGeneratedFiles(t *testing.T) {
	// Table SQL Builder files
	testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/table", "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "film_text.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go", "table_use_schema.go")

	testutils.AssertFileContent(t, genTestDir3+"/dvds/table/actor.go", actorSQLBuilderFile)
	testutils.AssertFileContent(t, genTestDir3+"/dvds/table/table_use_schema.go", tableUseSchemaFile)

	// View SQL Builder files
	testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/view", "actor_info.go", "film_list.go", "nicer_but_slower_film_list.go",
		"sales_by_film_category.go", "customer_list.go", "sales_by_store.go", "staff_list.go", "view_use_schema.go")

	testutils.AssertFileContent(t, genTestDir3+"/dvds/view/actor_info.go", actorInfoSQLBuilderFile)
	testutils.AssertFileContent(t, genTestDir3+"/dvds/view/view_use_schema.go", viewUseSchemaFile)

	// Enums SQL Builder files
	testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/enum", "film_rating.go", "film_list_rating.go", "nicer_but_slower_film_list_rating.go")
	testutils.AssertFileContent(t, genTestDir3+"/dvds/enum/film_rating.go", mpaaRatingEnumFile)

	// Model files
	testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/model", "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "film_text.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go",
		"film_rating.go", "film_list_rating.go", "nicer_but_slower_film_list_rating.go",
		"actor_info.go", "film_list.go", "nicer_but_slower_film_list.go", "sales_by_film_category.go",
		"customer_list.go", "sales_by_store.go", "staff_list.go")

	testutils.AssertFileContent(t, genTestDir3+"/dvds/model/actor.go", actorModelFile)
}

func TestModelColumnComment(t *testing.T) {
	if common.IsHack() {
		t.SkipNow()
	}
	testutils.AssertFileContent(t, "./../.gentestdata/mysql/test_sample/model/link.go", `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package model

type Link struct {
	ID          int32   `+"`sql:\"primary_key\"`"+` // this is link id
	URL         string  // link url
	Name        string  // Unicode characters comment ₲鬼佬℧⇄↻
	Description *string // '"\\%\_
}
`)
}

func TestSQLBuilderColumnComment(t *testing.T) {
	if common.IsHack() {
		t.SkipNow()
	}
	testutils.AssertFileContent(t, "./../.gentestdata/mysql/test_sample/table/link.go", `
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

var Link = newLinkTable("test_sample", "link", "")

type linkTable struct {
	mysql.Table

	// Columns
	ID          mysql.ColumnInteger // this is link id
	URL         mysql.ColumnString  // link url
	Name        mysql.ColumnString  // Unicode characters comment ₲鬼佬℧⇄↻
	Description mysql.ColumnString  // '"\\%\_

	AllColumns     mysql.ColumnList
	MutableColumns mysql.ColumnList
}

type LinkTable struct {
	linkTable

	NEW linkTable
}

// AS creates new LinkTable with assigned alias
func (a LinkTable) AS(alias string) *LinkTable {
	return newLinkTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new LinkTable with assigned schema name
func (a LinkTable) FromSchema(schemaName string) *LinkTable {
	return newLinkTable(schemaName, a.TableName(), a.Alias())
}

// WithPrefix creates new LinkTable with assigned table prefix
func (a LinkTable) WithPrefix(prefix string) *LinkTable {
	return newLinkTable(a.SchemaName(), prefix+a.TableName(), a.TableName())
}

// WithSuffix creates new LinkTable with assigned table suffix
func (a LinkTable) WithSuffix(suffix string) *LinkTable {
	return newLinkTable(a.SchemaName(), a.TableName()+suffix, a.TableName())
}

func newLinkTable(schemaName, tableName, alias string) *LinkTable {
	return &LinkTable{
		linkTable: newLinkTableImpl(schemaName, tableName, alias),
		NEW:       newLinkTableImpl("", "new", ""),
	}
}

func newLinkTableImpl(schemaName, tableName, alias string) linkTable {
	var (
		IDColumn          = mysql.IntegerColumn("id")
		URLColumn         = mysql.StringColumn("url")
		NameColumn        = mysql.StringColumn("name")
		DescriptionColumn = mysql.StringColumn("description")
		allColumns        = mysql.ColumnList{IDColumn, URLColumn, NameColumn, DescriptionColumn}
		mutableColumns    = mysql.ColumnList{URLColumn, NameColumn, DescriptionColumn}
	)

	return linkTable{
		Table: mysql.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		ID:          IDColumn,
		URL:         URLColumn,
		Name:        NameColumn,
		Description: DescriptionColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
`)
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

type actorTable struct {
	mysql.Table

	// Columns
	ActorID    mysql.ColumnInteger
	FirstName  mysql.ColumnString
	LastName   mysql.ColumnString
	LastUpdate mysql.ColumnTimestamp

	AllColumns     mysql.ColumnList
	MutableColumns mysql.ColumnList
}

type ActorTable struct {
	actorTable

	NEW actorTable
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
		NEW:        newActorTableImpl("", "new", ""),
	}
}

func newActorTableImpl(schemaName, tableName, alias string) actorTable {
	var (
		ActorIDColumn    = mysql.IntegerColumn("actor_id")
		FirstNameColumn  = mysql.StringColumn("first_name")
		LastNameColumn   = mysql.StringColumn("last_name")
		LastUpdateColumn = mysql.TimestampColumn("last_update")
		allColumns       = mysql.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, LastUpdateColumn}
		mutableColumns   = mysql.ColumnList{FirstNameColumn, LastNameColumn, LastUpdateColumn}
	)

	return actorTable{
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

var tableUseSchemaFile = `
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

type actorInfoTable struct {
	mysql.Table

	// Columns
	ActorID   mysql.ColumnInteger
	FirstName mysql.ColumnString
	LastName  mysql.ColumnString
	FilmInfo  mysql.ColumnString

	AllColumns     mysql.ColumnList
	MutableColumns mysql.ColumnList
}

type ActorInfoTable struct {
	actorInfoTable

	NEW actorInfoTable
}

// AS creates new ActorInfoTable with assigned alias
func (a ActorInfoTable) AS(alias string) *ActorInfoTable {
	return newActorInfoTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new ActorInfoTable with assigned schema name
func (a ActorInfoTable) FromSchema(schemaName string) *ActorInfoTable {
	return newActorInfoTable(schemaName, a.TableName(), a.Alias())
}

// WithPrefix creates new ActorInfoTable with assigned table prefix
func (a ActorInfoTable) WithPrefix(prefix string) *ActorInfoTable {
	return newActorInfoTable(a.SchemaName(), prefix+a.TableName(), a.TableName())
}

// WithSuffix creates new ActorInfoTable with assigned table suffix
func (a ActorInfoTable) WithSuffix(suffix string) *ActorInfoTable {
	return newActorInfoTable(a.SchemaName(), a.TableName()+suffix, a.TableName())
}

func newActorInfoTable(schemaName, tableName, alias string) *ActorInfoTable {
	return &ActorInfoTable{
		actorInfoTable: newActorInfoTableImpl(schemaName, tableName, alias),
		NEW:            newActorInfoTableImpl("", "new", ""),
	}
}

func newActorInfoTableImpl(schemaName, tableName, alias string) actorInfoTable {
	var (
		ActorIDColumn   = mysql.IntegerColumn("actor_id")
		FirstNameColumn = mysql.StringColumn("first_name")
		LastNameColumn  = mysql.StringColumn("last_name")
		FilmInfoColumn  = mysql.StringColumn("film_info")
		allColumns      = mysql.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, FilmInfoColumn}
		mutableColumns  = mysql.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, FilmInfoColumn}
	)

	return actorInfoTable{
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
var viewUseSchemaFile = `
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
	ActorInfo = ActorInfo.FromSchema(schema)
	CustomerList = CustomerList.FromSchema(schema)
	FilmList = FilmList.FromSchema(schema)
	NicerButSlowerFilmList = NicerButSlowerFilmList.FromSchema(schema)
	SalesByFilmCategory = SalesByFilmCategory.FromSchema(schema)
	SalesByStore = SalesByStore.FromSchema(schema)
	StaffList = StaffList.FromSchema(schema)
}
`
