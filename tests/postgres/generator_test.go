package postgres

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/postgres"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/testutils"
	postgres2 "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	"github.com/go-jet/jet/v2/tests/internal/utils/file"
)

func dsn(host string, port int, dbName, user, password string) string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		user,
		password,
		host,
		port,
		dbName,
	)
}

func defaultDSN() string {
	return dsn(
		PgHost,
		PgPort,
		PgDBName,
		PgUser,
		PgPassword,
	)
}

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

	require.Equal(t, reflect.TypeOf(filmActor.FilmID).String(), "int16")
	filmIDField, ok := reflect.TypeOf(filmActor).FieldByName("FilmID")
	require.True(t, ok)
	require.Equal(t, filmIDField.Tag.Get("sql"), "primary_key")

	require.Equal(t, reflect.TypeOf(filmActor.ActorID).String(), "int16")
	actorIDField, ok = reflect.TypeOf(filmActor).FieldByName("ActorID")
	require.True(t, ok)
	require.Equal(t, filmIDField.Tag.Get("sql"), "primary_key")

	staff := model.Staff{}

	require.Equal(t, reflect.TypeOf(staff.Email).String(), "*string")
	require.Equal(t, reflect.TypeOf(staff.Picture).String(), "*[]uint8")
}

const genTestDir2 = "./.gentestdata2"

func getEnvironmentPort() string {
	var port string
	if sourceIsCockroachDB() {
		port = strconv.Itoa(CockroachPort)
	} else {
		port = strconv.Itoa(PgPort)
	}
	return port

}

func TestCmdGenerator(t *testing.T) {
	skipForCockroachDB(t)
	err := os.RemoveAll(genTestDir2)
	require.NoError(t, err)

	cmd := exec.Command("jet", "-source=PostgreSQL", "-dbname=jetdb", "-host=localhost",
		"-port="+strconv.Itoa(PgPort),
		"-user=jet",
		"-password=jet",
		"-schema=dvds",
		"-path="+genTestDir2)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Run()
	require.NoError(t, err)

	assertGeneratedFiles(t)

	err = os.RemoveAll(genTestDir2)
	require.NoError(t, err)

	// Check that connection via DSN works
	cmd = exec.Command("jet", "-dsn="+defaultDSN(), "-schema=dvds", "-path="+genTestDir2)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Run()
	require.NoError(t, err)

	assertGeneratedFiles(t)

	err = os.RemoveAll(genTestDir2)
	require.NoError(t, err)
}

func TestGeneratorIgnoreTables(t *testing.T) {
	skipForCockroachDB(t)
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "with dsn",
			args: []string{
				"-dsn=" + defaultDSN(),
				"-schema=dvds",
				"-ignore-tables=actor,ADDRESS,country, Film , cITY,",
				"-ignore-views=Actor_info, FILM_LIST ,staff_list",
				"-ignore-enums=mpaa_rating",
				"-path=" + genTestDir2,
			},
		},
		{
			name: "without dsn",
			args: []string{
				"-source=PostgreSQL",
				"-host=localhost",
				"-port=" + getEnvironmentPort(),
				"-user=jet",
				"-password=jet",
				"-dbname=jetdb",
				"-schema=dvds",
				"-ignore-tables=actor,ADDRESS,country, Film , cITY,",
				"-ignore-views=Actor_info, FILM_LIST ,staff_list",
				"-ignore-enums=mpaa_rating",
				"-path=" + genTestDir2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := os.RemoveAll(genTestDir2)
			require.NoError(t, err)

			cmd := exec.Command("jet", tt.args...)

			fmt.Println(cmd.Args)
			cmd.Stderr = os.Stderr
			cmd.Stdout = os.Stdout

			err = cmd.Run()
			require.NoError(t, err)

			// Table SQL Builder files
			testutils.AssertFileNamesEqual(t, "./.gentestdata2/jetdb/dvds/table",
				"category.go", "customer.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
				"payment.go", "rental.go", "staff.go", "store.go", "table_use_schema.go")

			// View SQL Builder files
			testutils.AssertFileNamesEqual(t, "./.gentestdata2/jetdb/dvds/view",
				"nicer_but_slower_film_list.go", "sales_by_film_category.go", "customer_list.go",
				"sales_by_store.go", "view_use_schema.go")

			// Enums SQL Builder files
			file.NotExists(t, "./.gentestdata2/jetdb/dvds/enum", "mpaa_rating.go")

			// Model files
			testutils.AssertFileNamesEqual(t, "./.gentestdata2/jetdb/dvds/model", "category.go",
				"customer.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
				"payment.go", "rental.go", "staff.go", "store.go",
				"nicer_but_slower_film_list.go", "sales_by_film_category.go",
				"customer_list.go", "sales_by_store.go")
		})
	}
}

func TestGenerator(t *testing.T) {
	skipForCockroachDB(t)

	for i := 0; i < 3; i++ {
		err := postgres.Generate(genTestDir2, postgres.DBConnection{
			Host:     PgHost,
			Port:     PgPort,
			User:     PgUser,
			Password: PgPassword,
			SslMode:  "disable",
			Params:   "",

			DBName:     PgDBName,
			SchemaName: "dvds",
		})

		require.NoError(t, err)

		assertGeneratedFiles(t)
	}

	for i := 0; i < 3; i++ {
		err := postgres.GenerateDSN(defaultDSN(), "dvds", genTestDir2)
		require.NoError(t, err)

		assertGeneratedFiles(t)
	}

	err := os.RemoveAll(genTestDir2)
	require.NoError(t, err)
}

func TestGenerator_TableMetadata(t *testing.T) {
	skipForCockroachDB(t)
	var schema metadata.Schema
	err := postgres.GenerateDSN(defaultDSN(), "dvds", genTestDir2,
		template.Default(postgres2.Dialect).UseSchema(func(m metadata.Schema) template.Schema {
			schema = m
			return template.DefaultSchema(m)
		}))
	require.NoError(t, err)

	// Spot check the actor table and assert that the emitted
	// properties are as expected.
	var got metadata.Table
	var specialFeatures metadata.Column
	for _, table := range schema.TablesMetaData {
		if table.Name == "actor" {
			got = table
		}
		if table.Name == "film" {
			for _, column := range table.Columns {
				if column.Name == "special_features" {
					specialFeatures = column
				}
			}
		}
	}

	want := metadata.Table{
		Name: "actor",
		Columns: []metadata.Column{
			{Name: "actor_id", IsPrimaryKey: true, IsNullable: false, IsGenerated: false, HasDefault: true, DataType: metadata.DataType{Name: "int4", Kind: "base", IsUnsigned: false}, Comment: ""},
			{Name: "first_name", IsPrimaryKey: false, IsNullable: false, IsGenerated: false, HasDefault: false, DataType: metadata.DataType{Name: "varchar", Kind: "base", IsUnsigned: false}, Comment: ""},
			{Name: "last_name", IsPrimaryKey: false, IsNullable: false, IsGenerated: false, HasDefault: false, DataType: metadata.DataType{Name: "varchar", Kind: "base", IsUnsigned: false}, Comment: ""},
			{Name: "last_update", IsPrimaryKey: false, IsNullable: false, IsGenerated: false, HasDefault: false, DataType: metadata.DataType{Name: "timestamp", Kind: "base", IsUnsigned: false}, Comment: ""},
		},
	}
	require.Equal(t, want, got)
	require.Equal(t, metadata.ArrayType, specialFeatures.DataType.Kind)
}

func TestGeneratorSpecialCharacters(t *testing.T) {
	t.SkipNow()
	err := postgres.Generate(genTestDir2, postgres.DBConnection{
		Host:     PgHost,
		Port:     PgPort,
		User:     "!@#$%^&* () {}[];+-",
		Password: "!@#$%^&* () {}[];+-",
		SslMode:  "disable",
		Params:   "",

		DBName:     "!@#$%^&* () {}[];+-",
		SchemaName: "!@#$%^&* () {}[];+-",
	})

	require.NoError(t, err)
}

func TestGenerateErrorCases(t *testing.T) {
	err := postgres.GenerateDSN("!@#$%&*", "", "")
	require.ErrorContains(t, err, "failed to parse as DSN")
	err = postgres.GenerateDSN(dsn(PgHost, -1, "!@!#", "", ""), "", "")
	require.ErrorContains(t, err, "invalid port")
	err = postgres.GenerateDSN(dsn(PgHost, PgPort, "!@!#", "", ""), "", "")
	require.ErrorContains(t, err, "failed to open db connection")
	//err = postgres.GenerateDSN(dsn(dbconfig.PgHost, dbconfig.PgPort, dbconfig.PgDBName, "", ""), "", "")
	//require.ErrorContains(t, err, "password authentication failed")
	//err = postgres.GenerateDSN(dsn(dbconfig.PgHost, dbconfig.PgPort, dbconfig.PgDBName, dbconfig.PgUser, ""), "", "")
	//require.ErrorContains(t, err, "password authentication failed for user \"jet\"")
}

func assertGeneratedFiles(t *testing.T) {
	// Table SQL Builder files
	testutils.AssertFileNamesEqual(t, "./.gentestdata2/jetdb/dvds/table",
		"actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go", "table_use_schema.go")

	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/table/actor.go", actorSQLBuilderFile)
	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/table/table_use_schema.go", tableUseSchemaFile)

	// View SQL Builder files
	testutils.AssertFileNamesEqual(t, "./.gentestdata2/jetdb/dvds/view",
		"actor_info.go", "film_list.go", "nicer_but_slower_film_list.go",
		"sales_by_film_category.go", "customer_list.go", "sales_by_store.go", "staff_list.go", "view_use_schema.go")

	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/view/actor_info.go", actorInfoSQLBuilderFile)
	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/view/view_use_schema.go", viewUseSchemaFile)

	// Enums SQL Builder files
	testutils.AssertFileNamesEqual(t, "./.gentestdata2/jetdb/dvds/enum", "mpaa_rating.go")
	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/enum/mpaa_rating.go", mpaaRatingEnumFile)

	// Model files
	testutils.AssertFileNamesEqual(t, "./.gentestdata2/jetdb/dvds/model", "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go", "mpaa_rating.go",
		"actor_info.go", "film_list.go", "nicer_but_slower_film_list.go", "sales_by_film_category.go",
		"customer_list.go", "sales_by_store.go", "staff_list.go")

	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/model/actor.go", actorModelFile)
}

var mpaaRatingEnumFile = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package enum

import "github.com/go-jet/jet/v2/postgres"

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
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package table

import (
	"github.com/go-jet/jet/v2/postgres"
)

var Actor = newActorTable("dvds", "actor", "")

type actorTable struct {
	postgres.Table

	// Columns
	ActorID    postgres.ColumnInteger
	FirstName  postgres.ColumnString
	LastName   postgres.ColumnString
	LastUpdate postgres.ColumnTimestamp

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
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
		ActorIDColumn    = postgres.IntegerColumn("actor_id")
		FirstNameColumn  = postgres.StringColumn("first_name")
		LastNameColumn   = postgres.StringColumn("last_name")
		LastUpdateColumn = postgres.TimestampColumn("last_update")
		allColumns       = postgres.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, LastUpdateColumn}
		mutableColumns   = postgres.ColumnList{FirstNameColumn, LastNameColumn, LastUpdateColumn}
	)

	return actorTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

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
	ActorID    int32 ` + "`sql:\"primary_key\"`" + `
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
	"github.com/go-jet/jet/v2/postgres"
)

var ActorInfo = newActorInfoTable("dvds", "actor_info", "")

type actorInfoTable struct {
	postgres.Table

	// Columns
	ActorID   postgres.ColumnInteger
	FirstName postgres.ColumnString
	LastName  postgres.ColumnString
	FilmInfo  postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type ActorInfoTable struct {
	actorInfoTable

	EXCLUDED actorInfoTable
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
		EXCLUDED:       newActorInfoTableImpl("", "excluded", ""),
	}
}

func newActorInfoTableImpl(schemaName, tableName, alias string) actorInfoTable {
	var (
		ActorIDColumn   = postgres.IntegerColumn("actor_id")
		FirstNameColumn = postgres.StringColumn("first_name")
		LastNameColumn  = postgres.StringColumn("last_name")
		FilmInfoColumn  = postgres.StringColumn("film_info")
		allColumns      = postgres.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, FilmInfoColumn}
		mutableColumns  = postgres.ColumnList{ActorIDColumn, FirstNameColumn, LastNameColumn, FilmInfoColumn}
	)

	return actorInfoTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

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

func TestGeneratedAllTypesSQLBuilderFiles(t *testing.T) {
	skipForCockroachDB(t) // because of rowid column

	enumDir := filepath.Join(testRoot, "/.gentestdata/jetdb/test_sample/enum/")
	modelDir := filepath.Join(testRoot, "/.gentestdata/jetdb/test_sample/model/")
	tableDir := filepath.Join(testRoot, "/.gentestdata/jetdb/test_sample/table/")
	viewDir := filepath.Join(testRoot, "/.gentestdata/jetdb/test_sample/view/")

	testutils.AssertFileNamesEqual(t, enumDir, "mood.go", "level.go")
	testutils.AssertFileContent(t, enumDir+"/mood.go", moodEnumContent)
	testutils.AssertFileContent(t, enumDir+"/level.go", levelEnumContent)

	testutils.AssertFileNamesEqual(t, modelDir, "all_types.go", "all_types_view.go", "employee.go", "link.go",
		"mood.go", "person.go", "person_phone.go", "weird_names_table.go", "level.go", "user.go", "floats.go", "people.go",
		"components.go", "vulnerabilities.go", "all_types_materialized_view.go", "sample_ranges.go")
	testutils.AssertFileContent(t, modelDir+"/all_types.go", allTypesModelContent)
	testutils.AssertFileContent(t, modelDir+"/link.go", linkModelContent)

	testutils.AssertFileNamesEqual(t, tableDir, "all_types.go", "employee.go", "link.go",
		"person.go", "person_phone.go", "weird_names_table.go", "user.go", "floats.go", "people.go", "table_use_schema.go",
		"components.go", "vulnerabilities.go", "sample_ranges.go")
	testutils.AssertFileContent(t, tableDir+"/all_types.go", allTypesTableContent)
	testutils.AssertFileContent(t, tableDir+"/sample_ranges.go", sampleRangeTableContent)

	testutils.AssertFileContent(t, tableDir+"/link.go", linkTableContent)

	testutils.AssertFileNamesEqual(t, viewDir, "all_types_materialized_view.go", "all_types_view.go",
		"view_use_schema.go")
}

var moodEnumContent = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package enum

import "github.com/go-jet/jet/v2/postgres"

var Mood = &struct {
	Sad   postgres.StringExpression
	Ok    postgres.StringExpression
	Happy postgres.StringExpression
}{
	Sad:   postgres.NewEnumValue("sad"),
	Ok:    postgres.NewEnumValue("ok"),
	Happy: postgres.NewEnumValue("happy"),
}
`

var levelEnumContent = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package enum

import "github.com/go-jet/jet/v2/postgres"

// Level enum
var Level = &struct {
	Level1 postgres.StringExpression
	Level2 postgres.StringExpression
	Level3 postgres.StringExpression
	Level4 postgres.StringExpression
	Level5 postgres.StringExpression
}{
	Level1: postgres.NewEnumValue("1"),
	Level2: postgres.NewEnumValue("2"),
	Level3: postgres.NewEnumValue("3"),
	Level4: postgres.NewEnumValue("4"),
	Level5: postgres.NewEnumValue("5"),
}
`

var allTypesModelContent = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package model

import (
	"github.com/google/uuid"
	"time"
)

type AllTypes struct {
	SmallIntPtr          *int16
	SmallInt             int16
	IntegerPtr           *int32
	Integer              int32
	BigIntPtr            *int64
	BigInt               int64
	DecimalPtr           *float64
	Decimal              float64
	NumericPtr           *float64
	Numeric              float64
	RealPtr              *float32
	Real                 float32
	DoublePrecisionPtr   *float64
	DoublePrecision      float64
	Smallserial          int16
	Serial               int32
	Bigserial            int64
	VarCharPtr           *string
	VarChar              string
	CharPtr              *string
	Char                 string
	TextPtr              *string
	Text                 string
	ByteaPtr             *[]byte
	Bytea                []byte
	TimestampzPtr        *time.Time
	Timestampz           time.Time
	TimestampPtr         *time.Time
	Timestamp            time.Time
	DatePtr              *time.Time
	Date                 time.Time
	TimezPtr             *time.Time
	Timez                time.Time
	TimePtr              *time.Time
	Time                 time.Time
	IntervalPtr          *string
	Interval             string
	BooleanPtr           *bool
	Boolean              bool
	PointPtr             *string
	BitPtr               *string
	Bit                  string
	BitVaryingPtr        *string
	BitVarying           string
	TsvectorPtr          *string
	Tsvector             string
	UUIDPtr              *uuid.UUID
	UUID                 uuid.UUID
	XMLPtr               *string
	XML                  string
	JSONPtr              *string
	JSON                 string
	JsonbPtr             *string
	Jsonb                string
	IntegerArrayPtr      *string
	IntegerArray         string
	TextArrayPtr         *string
	TextArray            string
	JsonbArray           string
	TextMultiDimArrayPtr *string
	TextMultiDimArray    string
	MoodPtr              *Mood
	Mood                 Mood
}
`

var linkModelContent = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package model

// Link table
type Link struct {
	ID          int64   ` + "`sql:\"primary_key\"`" + ` // this is link id
	URL         string  // link url
	Name        string  // Unicode characters comment ₲鬼佬℧⇄↻
	Description *string // '"Z\%_
}
`

var allTypesTableContent = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package table

import (
	"github.com/go-jet/jet/v2/postgres"
)

var AllTypes = newAllTypesTable("test_sample", "all_types", "")

type allTypesTable struct {
	postgres.Table

	// Columns
	SmallIntPtr          postgres.ColumnInteger
	SmallInt             postgres.ColumnInteger
	IntegerPtr           postgres.ColumnInteger
	Integer              postgres.ColumnInteger
	BigIntPtr            postgres.ColumnInteger
	BigInt               postgres.ColumnInteger
	DecimalPtr           postgres.ColumnFloat
	Decimal              postgres.ColumnFloat
	NumericPtr           postgres.ColumnFloat
	Numeric              postgres.ColumnFloat
	RealPtr              postgres.ColumnFloat
	Real                 postgres.ColumnFloat
	DoublePrecisionPtr   postgres.ColumnFloat
	DoublePrecision      postgres.ColumnFloat
	Smallserial          postgres.ColumnInteger
	Serial               postgres.ColumnInteger
	Bigserial            postgres.ColumnInteger
	VarCharPtr           postgres.ColumnString
	VarChar              postgres.ColumnString
	CharPtr              postgres.ColumnString
	Char                 postgres.ColumnString
	TextPtr              postgres.ColumnString
	Text                 postgres.ColumnString
	ByteaPtr             postgres.ColumnString
	Bytea                postgres.ColumnString
	TimestampzPtr        postgres.ColumnTimestampz
	Timestampz           postgres.ColumnTimestampz
	TimestampPtr         postgres.ColumnTimestamp
	Timestamp            postgres.ColumnTimestamp
	DatePtr              postgres.ColumnDate
	Date                 postgres.ColumnDate
	TimezPtr             postgres.ColumnTimez
	Timez                postgres.ColumnTimez
	TimePtr              postgres.ColumnTime
	Time                 postgres.ColumnTime
	IntervalPtr          postgres.ColumnInterval
	Interval             postgres.ColumnInterval
	BooleanPtr           postgres.ColumnBool
	Boolean              postgres.ColumnBool
	PointPtr             postgres.ColumnString
	BitPtr               postgres.ColumnString
	Bit                  postgres.ColumnString
	BitVaryingPtr        postgres.ColumnString
	BitVarying           postgres.ColumnString
	TsvectorPtr          postgres.ColumnString
	Tsvector             postgres.ColumnString
	UUIDPtr              postgres.ColumnString
	UUID                 postgres.ColumnString
	XMLPtr               postgres.ColumnString
	XML                  postgres.ColumnString
	JSONPtr              postgres.ColumnString
	JSON                 postgres.ColumnString
	JsonbPtr             postgres.ColumnString
	Jsonb                postgres.ColumnString
	IntegerArrayPtr      postgres.ColumnString
	IntegerArray         postgres.ColumnString
	TextArrayPtr         postgres.ColumnString
	TextArray            postgres.ColumnString
	JsonbArray           postgres.ColumnString
	TextMultiDimArrayPtr postgres.ColumnString
	TextMultiDimArray    postgres.ColumnString
	MoodPtr              postgres.ColumnString
	Mood                 postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type AllTypesTable struct {
	allTypesTable

	EXCLUDED allTypesTable
}

// AS creates new AllTypesTable with assigned alias
func (a AllTypesTable) AS(alias string) *AllTypesTable {
	return newAllTypesTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new AllTypesTable with assigned schema name
func (a AllTypesTable) FromSchema(schemaName string) *AllTypesTable {
	return newAllTypesTable(schemaName, a.TableName(), a.Alias())
}

// WithPrefix creates new AllTypesTable with assigned table prefix
func (a AllTypesTable) WithPrefix(prefix string) *AllTypesTable {
	return newAllTypesTable(a.SchemaName(), prefix+a.TableName(), a.TableName())
}

// WithSuffix creates new AllTypesTable with assigned table suffix
func (a AllTypesTable) WithSuffix(suffix string) *AllTypesTable {
	return newAllTypesTable(a.SchemaName(), a.TableName()+suffix, a.TableName())
}

func newAllTypesTable(schemaName, tableName, alias string) *AllTypesTable {
	return &AllTypesTable{
		allTypesTable: newAllTypesTableImpl(schemaName, tableName, alias),
		EXCLUDED:      newAllTypesTableImpl("", "excluded", ""),
	}
}

func newAllTypesTableImpl(schemaName, tableName, alias string) allTypesTable {
	var (
		SmallIntPtrColumn          = postgres.IntegerColumn("small_int_ptr")
		SmallIntColumn             = postgres.IntegerColumn("small_int")
		IntegerPtrColumn           = postgres.IntegerColumn("integer_ptr")
		IntegerColumn              = postgres.IntegerColumn("integer")
		BigIntPtrColumn            = postgres.IntegerColumn("big_int_ptr")
		BigIntColumn               = postgres.IntegerColumn("big_int")
		DecimalPtrColumn           = postgres.FloatColumn("decimal_ptr")
		DecimalColumn              = postgres.FloatColumn("decimal")
		NumericPtrColumn           = postgres.FloatColumn("numeric_ptr")
		NumericColumn              = postgres.FloatColumn("numeric")
		RealPtrColumn              = postgres.FloatColumn("real_ptr")
		RealColumn                 = postgres.FloatColumn("real")
		DoublePrecisionPtrColumn   = postgres.FloatColumn("double_precision_ptr")
		DoublePrecisionColumn      = postgres.FloatColumn("double_precision")
		SmallserialColumn          = postgres.IntegerColumn("smallserial")
		SerialColumn               = postgres.IntegerColumn("serial")
		BigserialColumn            = postgres.IntegerColumn("bigserial")
		VarCharPtrColumn           = postgres.StringColumn("var_char_ptr")
		VarCharColumn              = postgres.StringColumn("var_char")
		CharPtrColumn              = postgres.StringColumn("char_ptr")
		CharColumn                 = postgres.StringColumn("char")
		TextPtrColumn              = postgres.StringColumn("text_ptr")
		TextColumn                 = postgres.StringColumn("text")
		ByteaPtrColumn             = postgres.StringColumn("bytea_ptr")
		ByteaColumn                = postgres.StringColumn("bytea")
		TimestampzPtrColumn        = postgres.TimestampzColumn("timestampz_ptr")
		TimestampzColumn           = postgres.TimestampzColumn("timestampz")
		TimestampPtrColumn         = postgres.TimestampColumn("timestamp_ptr")
		TimestampColumn            = postgres.TimestampColumn("timestamp")
		DatePtrColumn              = postgres.DateColumn("date_ptr")
		DateColumn                 = postgres.DateColumn("date")
		TimezPtrColumn             = postgres.TimezColumn("timez_ptr")
		TimezColumn                = postgres.TimezColumn("timez")
		TimePtrColumn              = postgres.TimeColumn("time_ptr")
		TimeColumn                 = postgres.TimeColumn("time")
		IntervalPtrColumn          = postgres.IntervalColumn("interval_ptr")
		IntervalColumn             = postgres.IntervalColumn("interval")
		BooleanPtrColumn           = postgres.BoolColumn("boolean_ptr")
		BooleanColumn              = postgres.BoolColumn("boolean")
		PointPtrColumn             = postgres.StringColumn("point_ptr")
		BitPtrColumn               = postgres.StringColumn("bit_ptr")
		BitColumn                  = postgres.StringColumn("bit")
		BitVaryingPtrColumn        = postgres.StringColumn("bit_varying_ptr")
		BitVaryingColumn           = postgres.StringColumn("bit_varying")
		TsvectorPtrColumn          = postgres.StringColumn("tsvector_ptr")
		TsvectorColumn             = postgres.StringColumn("tsvector")
		UUIDPtrColumn              = postgres.StringColumn("uuid_ptr")
		UUIDColumn                 = postgres.StringColumn("uuid")
		XMLPtrColumn               = postgres.StringColumn("xml_ptr")
		XMLColumn                  = postgres.StringColumn("xml")
		JSONPtrColumn              = postgres.StringColumn("json_ptr")
		JSONColumn                 = postgres.StringColumn("json")
		JsonbPtrColumn             = postgres.StringColumn("jsonb_ptr")
		JsonbColumn                = postgres.StringColumn("jsonb")
		IntegerArrayPtrColumn      = postgres.StringColumn("integer_array_ptr")
		IntegerArrayColumn         = postgres.StringColumn("integer_array")
		TextArrayPtrColumn         = postgres.StringColumn("text_array_ptr")
		TextArrayColumn            = postgres.StringColumn("text_array")
		JsonbArrayColumn           = postgres.StringColumn("jsonb_array")
		TextMultiDimArrayPtrColumn = postgres.StringColumn("text_multi_dim_array_ptr")
		TextMultiDimArrayColumn    = postgres.StringColumn("text_multi_dim_array")
		MoodPtrColumn              = postgres.StringColumn("mood_ptr")
		MoodColumn                 = postgres.StringColumn("mood")
		allColumns                 = postgres.ColumnList{SmallIntPtrColumn, SmallIntColumn, IntegerPtrColumn, IntegerColumn, BigIntPtrColumn, BigIntColumn, DecimalPtrColumn, DecimalColumn, NumericPtrColumn, NumericColumn, RealPtrColumn, RealColumn, DoublePrecisionPtrColumn, DoublePrecisionColumn, SmallserialColumn, SerialColumn, BigserialColumn, VarCharPtrColumn, VarCharColumn, CharPtrColumn, CharColumn, TextPtrColumn, TextColumn, ByteaPtrColumn, ByteaColumn, TimestampzPtrColumn, TimestampzColumn, TimestampPtrColumn, TimestampColumn, DatePtrColumn, DateColumn, TimezPtrColumn, TimezColumn, TimePtrColumn, TimeColumn, IntervalPtrColumn, IntervalColumn, BooleanPtrColumn, BooleanColumn, PointPtrColumn, BitPtrColumn, BitColumn, BitVaryingPtrColumn, BitVaryingColumn, TsvectorPtrColumn, TsvectorColumn, UUIDPtrColumn, UUIDColumn, XMLPtrColumn, XMLColumn, JSONPtrColumn, JSONColumn, JsonbPtrColumn, JsonbColumn, IntegerArrayPtrColumn, IntegerArrayColumn, TextArrayPtrColumn, TextArrayColumn, JsonbArrayColumn, TextMultiDimArrayPtrColumn, TextMultiDimArrayColumn, MoodPtrColumn, MoodColumn}
		mutableColumns             = postgres.ColumnList{SmallIntPtrColumn, SmallIntColumn, IntegerPtrColumn, IntegerColumn, BigIntPtrColumn, BigIntColumn, DecimalPtrColumn, DecimalColumn, NumericPtrColumn, NumericColumn, RealPtrColumn, RealColumn, DoublePrecisionPtrColumn, DoublePrecisionColumn, SmallserialColumn, SerialColumn, BigserialColumn, VarCharPtrColumn, VarCharColumn, CharPtrColumn, CharColumn, TextPtrColumn, TextColumn, ByteaPtrColumn, ByteaColumn, TimestampzPtrColumn, TimestampzColumn, TimestampPtrColumn, TimestampColumn, DatePtrColumn, DateColumn, TimezPtrColumn, TimezColumn, TimePtrColumn, TimeColumn, IntervalPtrColumn, IntervalColumn, BooleanPtrColumn, BooleanColumn, PointPtrColumn, BitPtrColumn, BitColumn, BitVaryingPtrColumn, BitVaryingColumn, TsvectorPtrColumn, TsvectorColumn, UUIDPtrColumn, UUIDColumn, XMLPtrColumn, XMLColumn, JSONPtrColumn, JSONColumn, JsonbPtrColumn, JsonbColumn, IntegerArrayPtrColumn, IntegerArrayColumn, TextArrayPtrColumn, TextArrayColumn, JsonbArrayColumn, TextMultiDimArrayPtrColumn, TextMultiDimArrayColumn, MoodPtrColumn, MoodColumn}
	)

	return allTypesTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		SmallIntPtr:          SmallIntPtrColumn,
		SmallInt:             SmallIntColumn,
		IntegerPtr:           IntegerPtrColumn,
		Integer:              IntegerColumn,
		BigIntPtr:            BigIntPtrColumn,
		BigInt:               BigIntColumn,
		DecimalPtr:           DecimalPtrColumn,
		Decimal:              DecimalColumn,
		NumericPtr:           NumericPtrColumn,
		Numeric:              NumericColumn,
		RealPtr:              RealPtrColumn,
		Real:                 RealColumn,
		DoublePrecisionPtr:   DoublePrecisionPtrColumn,
		DoublePrecision:      DoublePrecisionColumn,
		Smallserial:          SmallserialColumn,
		Serial:               SerialColumn,
		Bigserial:            BigserialColumn,
		VarCharPtr:           VarCharPtrColumn,
		VarChar:              VarCharColumn,
		CharPtr:              CharPtrColumn,
		Char:                 CharColumn,
		TextPtr:              TextPtrColumn,
		Text:                 TextColumn,
		ByteaPtr:             ByteaPtrColumn,
		Bytea:                ByteaColumn,
		TimestampzPtr:        TimestampzPtrColumn,
		Timestampz:           TimestampzColumn,
		TimestampPtr:         TimestampPtrColumn,
		Timestamp:            TimestampColumn,
		DatePtr:              DatePtrColumn,
		Date:                 DateColumn,
		TimezPtr:             TimezPtrColumn,
		Timez:                TimezColumn,
		TimePtr:              TimePtrColumn,
		Time:                 TimeColumn,
		IntervalPtr:          IntervalPtrColumn,
		Interval:             IntervalColumn,
		BooleanPtr:           BooleanPtrColumn,
		Boolean:              BooleanColumn,
		PointPtr:             PointPtrColumn,
		BitPtr:               BitPtrColumn,
		Bit:                  BitColumn,
		BitVaryingPtr:        BitVaryingPtrColumn,
		BitVarying:           BitVaryingColumn,
		TsvectorPtr:          TsvectorPtrColumn,
		Tsvector:             TsvectorColumn,
		UUIDPtr:              UUIDPtrColumn,
		UUID:                 UUIDColumn,
		XMLPtr:               XMLPtrColumn,
		XML:                  XMLColumn,
		JSONPtr:              JSONPtrColumn,
		JSON:                 JSONColumn,
		JsonbPtr:             JsonbPtrColumn,
		Jsonb:                JsonbColumn,
		IntegerArrayPtr:      IntegerArrayPtrColumn,
		IntegerArray:         IntegerArrayColumn,
		TextArrayPtr:         TextArrayPtrColumn,
		TextArray:            TextArrayColumn,
		JsonbArray:           JsonbArrayColumn,
		TextMultiDimArrayPtr: TextMultiDimArrayPtrColumn,
		TextMultiDimArray:    TextMultiDimArrayColumn,
		MoodPtr:              MoodPtrColumn,
		Mood:                 MoodColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
`

var sampleRangeTableContent = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package table

import (
	"github.com/go-jet/jet/v2/postgres"
)

var SampleRanges = newSampleRangesTable("test_sample", "sample_ranges", "")

type sampleRangesTable struct {
	postgres.Table

	// Columns
	DateRange       postgres.ColumnDateRange
	TimestampRange  postgres.ColumnTimestampRange
	TimestampzRange postgres.ColumnTimestampzRange
	Int4Range       postgres.ColumnInt4Range
	Int8Range       postgres.ColumnInt8Range
	NumRange        postgres.ColumnNumericRange

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type SampleRangesTable struct {
	sampleRangesTable

	EXCLUDED sampleRangesTable
}

// AS creates new SampleRangesTable with assigned alias
func (a SampleRangesTable) AS(alias string) *SampleRangesTable {
	return newSampleRangesTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new SampleRangesTable with assigned schema name
func (a SampleRangesTable) FromSchema(schemaName string) *SampleRangesTable {
	return newSampleRangesTable(schemaName, a.TableName(), a.Alias())
}

// WithPrefix creates new SampleRangesTable with assigned table prefix
func (a SampleRangesTable) WithPrefix(prefix string) *SampleRangesTable {
	return newSampleRangesTable(a.SchemaName(), prefix+a.TableName(), a.TableName())
}

// WithSuffix creates new SampleRangesTable with assigned table suffix
func (a SampleRangesTable) WithSuffix(suffix string) *SampleRangesTable {
	return newSampleRangesTable(a.SchemaName(), a.TableName()+suffix, a.TableName())
}

func newSampleRangesTable(schemaName, tableName, alias string) *SampleRangesTable {
	return &SampleRangesTable{
		sampleRangesTable: newSampleRangesTableImpl(schemaName, tableName, alias),
		EXCLUDED:          newSampleRangesTableImpl("", "excluded", ""),
	}
}

func newSampleRangesTableImpl(schemaName, tableName, alias string) sampleRangesTable {
	var (
		DateRangeColumn       = postgres.DateRangeColumn("date_range")
		TimestampRangeColumn  = postgres.TimestampRangeColumn("timestamp_range")
		TimestampzRangeColumn = postgres.TimestampzRangeColumn("timestampz_range")
		Int4RangeColumn       = postgres.Int4RangeColumn("int4_range")
		Int8RangeColumn       = postgres.Int8RangeColumn("int8_range")
		NumRangeColumn        = postgres.NumericRangeColumn("num_range")
		allColumns            = postgres.ColumnList{DateRangeColumn, TimestampRangeColumn, TimestampzRangeColumn, Int4RangeColumn, Int8RangeColumn, NumRangeColumn}
		mutableColumns        = postgres.ColumnList{DateRangeColumn, TimestampRangeColumn, TimestampzRangeColumn, Int4RangeColumn, Int8RangeColumn, NumRangeColumn}
	)

	return sampleRangesTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		DateRange:       DateRangeColumn,
		TimestampRange:  TimestampRangeColumn,
		TimestampzRange: TimestampzRangeColumn,
		Int4Range:       Int4RangeColumn,
		Int8Range:       Int8RangeColumn,
		NumRange:        NumRangeColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
`

var linkTableContent = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package table

import (
	"github.com/go-jet/jet/v2/postgres"
)

var Link = newLinkTable("test_sample", "link", "")

// Link table
type linkTable struct {
	postgres.Table

	// Columns
	ID          postgres.ColumnInteger // this is link id
	URL         postgres.ColumnString  // link url
	Name        postgres.ColumnString  // Unicode characters comment ₲鬼佬℧⇄↻
	Description postgres.ColumnString  // '"Z\%_

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type LinkTable struct {
	linkTable

	EXCLUDED linkTable
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
		EXCLUDED:  newLinkTableImpl("", "excluded", ""),
	}
}

func newLinkTableImpl(schemaName, tableName, alias string) linkTable {
	var (
		IDColumn          = postgres.IntegerColumn("id")
		URLColumn         = postgres.StringColumn("url")
		NameColumn        = postgres.StringColumn("name")
		DescriptionColumn = postgres.StringColumn("description")
		allColumns        = postgres.ColumnList{IDColumn, URLColumn, NameColumn, DescriptionColumn}
		mutableColumns    = postgres.ColumnList{URLColumn, NameColumn, DescriptionColumn}
	)

	return linkTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		ID:          IDColumn,
		URL:         URLColumn,
		Name:        NameColumn,
		Description: DescriptionColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
`
