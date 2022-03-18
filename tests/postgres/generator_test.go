package postgres

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/go-jet/jet/v2/generator/postgres"
	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
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

func TestCmdGenerator(t *testing.T) {
	err := os.RemoveAll(genTestDir2)
	require.NoError(t, err)

	cmd := exec.Command("jet", "-source=PostgreSQL", "-dbname=jetdb", "-host=localhost",
		"-port="+strconv.Itoa(dbconfig.PgPort),
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
	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		dbconfig.PgUser,
		dbconfig.PgPassword,
		dbconfig.PgHost,
		dbconfig.PgPort,
		"jetdb",
	)
	cmd = exec.Command("jet", "-dsn="+dsn, "-schema=dvds", "-path="+genTestDir2)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Run()
	require.NoError(t, err)

	assertGeneratedFiles(t)

	err = os.RemoveAll(genTestDir2)
	require.NoError(t, err)
}

func TestGeneratorIgnoreTables(t *testing.T) {
	err := os.RemoveAll(genTestDir2)
	require.NoError(t, err)

	cmd := exec.Command("jet",
		"-source=PostgreSQL",
		"-host=localhost",
		"-port="+strconv.Itoa(dbconfig.PgPort),
		"-user=jet",
		"-password=jet",
		"-dbname=jetdb",
		"-schema=dvds",
		"-ignore-tables=actor,ADDRESS,country, Film , cITY,",
		"-ignore-views=Actor_info, FILM_LIST ,staff_list",
		"-ignore-enums=mpaa_rating",
		"-path="+genTestDir2)

	fmt.Println(cmd.Args)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Run()
	require.NoError(t, err)

	// Table SQL Builder files
	tableSQLBuilderFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/table")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, tableSQLBuilderFiles, "category.go",
		"customer.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go")

	// View SQL Builder files
	viewSQLBuilderFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/view")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, viewSQLBuilderFiles, "nicer_but_slower_film_list.go",
		"sales_by_film_category.go", "customer_list.go", "sales_by_store.go")

	// Enums SQL Builder files
	_, err = ioutil.ReadDir("./.gentestdata2/jetdb/dvds/enum")
	require.Error(t, err, "open ./.gentestdata2/jetdb/dvds/enum: no such file or directory")

	modelFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/model")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, modelFiles, "category.go",
		"customer.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go",
		"nicer_but_slower_film_list.go", "sales_by_film_category.go",
		"customer_list.go", "sales_by_store.go")
}

func TestGenerator(t *testing.T) {

	for i := 0; i < 3; i++ {
		err := postgres.Generate(genTestDir2, postgres.DBConnection{
			Host:     dbconfig.PgHost,
			Port:     dbconfig.PgPort,
			User:     dbconfig.PgUser,
			Password: dbconfig.PgPassword,
			SslMode:  "disable",
			Params:   "",

			DBName:     dbconfig.PgDBName,
			SchemaName: "dvds",
		})

		require.NoError(t, err)

		assertGeneratedFiles(t)
	}

	for i := 0; i < 3; i++ {
		dsn := fmt.Sprintf("postgresql://%[1]s:%[2]s@%[3]s:%[4]d/%[5]s?sslmode=disable",
			dbconfig.PgUser,
			dbconfig.PgPassword,
			dbconfig.PgHost,
			dbconfig.PgPort,
			dbconfig.PgDBName,
		)
		err := postgres.GenerateDSN(dsn, "dvds", genTestDir2)

		require.NoError(t, err)

		assertGeneratedFiles(t)
	}

	err := os.RemoveAll(genTestDir2)
	require.NoError(t, err)
}

func TestGeneratorSpecialCharacters(t *testing.T) {
	t.SkipNow()
	err := postgres.Generate(genTestDir2, postgres.DBConnection{
		Host:     dbconfig.PgHost,
		Port:     dbconfig.PgPort,
		User:     "!@#$%^&* () {}[];+-",
		Password: "!@#$%^&* () {}[];+-",
		SslMode:  "disable",
		Params:   "",

		DBName:     "!@#$%^&* () {}[];+-",
		SchemaName: "!@#$%^&* () {}[];+-",
	})

	require.NoError(t, err)
}

func assertGeneratedFiles(t *testing.T) {
	// Table SQL Builder files
	tableSQLBuilderFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/table")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, tableSQLBuilderFiles, "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go")

	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/table/actor.go", actorSQLBuilderFile)

	// View SQL Builder files
	viewSQLBuilderFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/view")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, viewSQLBuilderFiles, "actor_info.go", "film_list.go", "nicer_but_slower_film_list.go",
		"sales_by_film_category.go", "customer_list.go", "sales_by_store.go", "staff_list.go")

	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/view/actor_info.go", actorInfoSQLBuilderFile)

	// Enums SQL Builder files
	enumFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/enum")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, enumFiles, "mpaa_rating.go")
	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/enum/mpaa_rating.go", mpaaRatingEnumFile)

	// Model files
	modelFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/model")
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, modelFiles, "actor.go", "address.go", "category.go", "city.go", "country.go",
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

	//Columns
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

	//Columns
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

func TestGeneratedAllTypesSQLBuilderFiles(t *testing.T) {
	enumDir := filepath.Join(testRoot, "/.gentestdata/jetdb/test_sample/enum/")
	modelDir := filepath.Join(testRoot, "/.gentestdata/jetdb/test_sample/model/")
	tableDir := filepath.Join(testRoot, "/.gentestdata/jetdb/test_sample/table/")

	enumFiles, err := ioutil.ReadDir(enumDir)
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, enumFiles, "mood.go", "level.go")
	testutils.AssertFileContent(t, enumDir+"/mood.go", moodEnumContent)
	testutils.AssertFileContent(t, enumDir+"/level.go", levelEnumContent)

	modelFiles, err := ioutil.ReadDir(modelDir)
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, modelFiles, "all_types.go", "all_types_view.go", "employee.go", "link.go",
		"mood.go", "person.go", "person_phone.go", "weird_names_table.go", "level.go", "user.go", "floats.go")

	testutils.AssertFileContent(t, modelDir+"/all_types.go", allTypesModelContent)

	tableFiles, err := ioutil.ReadDir(tableDir)
	require.NoError(t, err)

	testutils.AssertFileNamesEqual(t, tableFiles, "all_types.go", "employee.go", "link.go",
		"person.go", "person_phone.go", "weird_names_table.go", "user.go", "floats.go")

	testutils.AssertFileContent(t, tableDir+"/all_types.go", allTypesTableContent)
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

	//Columns
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
		allColumns                 = postgres.ColumnList{SmallIntPtrColumn, SmallIntColumn, IntegerPtrColumn, IntegerColumn, BigIntPtrColumn, BigIntColumn, DecimalPtrColumn, DecimalColumn, NumericPtrColumn, NumericColumn, RealPtrColumn, RealColumn, DoublePrecisionPtrColumn, DoublePrecisionColumn, SmallserialColumn, SerialColumn, BigserialColumn, VarCharPtrColumn, VarCharColumn, CharPtrColumn, CharColumn, TextPtrColumn, TextColumn, ByteaPtrColumn, ByteaColumn, TimestampzPtrColumn, TimestampzColumn, TimestampPtrColumn, TimestampColumn, DatePtrColumn, DateColumn, TimezPtrColumn, TimezColumn, TimePtrColumn, TimeColumn, IntervalPtrColumn, IntervalColumn, BooleanPtrColumn, BooleanColumn, PointPtrColumn, BitPtrColumn, BitColumn, BitVaryingPtrColumn, BitVaryingColumn, TsvectorPtrColumn, TsvectorColumn, UUIDPtrColumn, UUIDColumn, XMLPtrColumn, XMLColumn, JSONPtrColumn, JSONColumn, JsonbPtrColumn, JsonbColumn, IntegerArrayPtrColumn, IntegerArrayColumn, TextArrayPtrColumn, TextArrayColumn, JsonbArrayColumn, TextMultiDimArrayPtrColumn, TextMultiDimArrayColumn}
		mutableColumns             = postgres.ColumnList{SmallIntPtrColumn, SmallIntColumn, IntegerPtrColumn, IntegerColumn, BigIntPtrColumn, BigIntColumn, DecimalPtrColumn, DecimalColumn, NumericPtrColumn, NumericColumn, RealPtrColumn, RealColumn, DoublePrecisionPtrColumn, DoublePrecisionColumn, SmallserialColumn, SerialColumn, BigserialColumn, VarCharPtrColumn, VarCharColumn, CharPtrColumn, CharColumn, TextPtrColumn, TextColumn, ByteaPtrColumn, ByteaColumn, TimestampzPtrColumn, TimestampzColumn, TimestampPtrColumn, TimestampColumn, DatePtrColumn, DateColumn, TimezPtrColumn, TimezColumn, TimePtrColumn, TimeColumn, IntervalPtrColumn, IntervalColumn, BooleanPtrColumn, BooleanColumn, PointPtrColumn, BitPtrColumn, BitColumn, BitVaryingPtrColumn, BitVaryingColumn, TsvectorPtrColumn, TsvectorColumn, UUIDPtrColumn, UUIDColumn, XMLPtrColumn, XMLColumn, JSONPtrColumn, JSONColumn, JsonbPtrColumn, JsonbColumn, IntegerArrayPtrColumn, IntegerArrayColumn, TextArrayPtrColumn, TextArrayColumn, JsonbArrayColumn, TextMultiDimArrayPtrColumn, TextMultiDimArrayColumn}
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

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
`
