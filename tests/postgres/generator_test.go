package postgres

import (
	"github.com/go-jet/jet/generator/postgres"
	"github.com/go-jet/jet/internal/testutils"
	"github.com/go-jet/jet/tests/dbconfig"
	"github.com/stretchr/testify/assert"
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
	assert.True(t, ok)
	assert.Equal(t, actorIDField.Tag.Get("sql"), "primary_key")
	assert.Equal(t, reflect.TypeOf(actor.FirstName).String(), "string")
	assert.Equal(t, reflect.TypeOf(actor.LastName).String(), "string")
	assert.Equal(t, reflect.TypeOf(actor.LastUpdate).String(), "time.Time")

	filmActor := model.FilmActor{}

	assert.Equal(t, reflect.TypeOf(filmActor.FilmID).String(), "int16")
	filmIDField, ok := reflect.TypeOf(filmActor).FieldByName("FilmID")
	assert.True(t, ok)
	assert.Equal(t, filmIDField.Tag.Get("sql"), "primary_key")

	assert.Equal(t, reflect.TypeOf(filmActor.ActorID).String(), "int16")
	actorIDField, ok = reflect.TypeOf(filmActor).FieldByName("ActorID")
	assert.True(t, ok)
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
	assert.NoError(t, err)

	err = os.RemoveAll(genTestDir2)
	assert.NoError(t, err)

	cmd := exec.Command("jet", "-source=PostgreSQL", "-dbname=jetdb", "-host=localhost", "-port=5432",
		"-user=jet", "-password=jet", "-schema=dvds", "-path="+genTestDir2)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Run()
	assert.NoError(t, err)

	assertGeneratedFiles(t)

	err = os.RemoveAll(genTestDir2)
	assert.NoError(t, err)
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

		assert.NoError(t, err)

		assertGeneratedFiles(t)
	}

	err := os.RemoveAll(genTestDir2)
	assert.NoError(t, err)
}

func assertGeneratedFiles(t *testing.T) {
	// Table SQL Builder files
	tableSQLBuilderFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/table")
	assert.NoError(t, err)

	testutils.AssertFileNamesEqual(t, tableSQLBuilderFiles, "actor.go", "address.go", "category.go", "city.go", "country.go",
		"customer.go", "film.go", "film_actor.go", "film_category.go", "inventory.go", "language.go",
		"payment.go", "rental.go", "staff.go", "store.go")

	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/table/actor.go", "\npackage table", actorSQLBuilderFile)

	// View SQL Builder files
	viewSQLBuilderFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/view")
	assert.NoError(t, err)

	testutils.AssertFileNamesEqual(t, viewSQLBuilderFiles, "actor_info.go", "film_list.go", "nicer_but_slower_film_list.go",
		"sales_by_film_category.go", "customer_list.go", "sales_by_store.go", "staff_list.go")

	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/view/actor_info.go", "\npackage view", actorInfoSQLBuilderFile)

	// Enums SQL Builder files
	enumFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/enum")
	assert.NoError(t, err)

	testutils.AssertFileNamesEqual(t, enumFiles, "mpaa_rating.go")
	testutils.AssertFileContent(t, "./.gentestdata2/jetdb/dvds/enum/mpaa_rating.go", "\npackage enum", mpaaRatingEnumFile)

	// Model files
	modelFiles, err := ioutil.ReadDir("./.gentestdata2/jetdb/dvds/model")
	assert.NoError(t, err)

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

func TestGeneratedAllTypesSQLBuilderFiles(t *testing.T) {
	enumDir := testRoot + ".gentestdata/jetdb/test_sample/enum/"
	modelDir := testRoot + ".gentestdata/jetdb/test_sample/model/"
	tableDir := testRoot + ".gentestdata/jetdb/test_sample/table/"

	enumFiles, err := ioutil.ReadDir(enumDir)
	assert.NoError(t, err)

	testutils.AssertFileNamesEqual(t, enumFiles, "mood.go", "level.go")
	testutils.AssertFileContent(t, enumDir+"mood.go", "\npackage enum", moodEnumContent)
	testutils.AssertFileContent(t, enumDir+"level.go", "\npackage enum", levelEnumContent)

	modelFiles, err := ioutil.ReadDir(modelDir)
	assert.NoError(t, err)

	testutils.AssertFileNamesEqual(t, modelFiles, "all_types.go", "all_types_view.go", "employee.go", "link.go",
		"mood.go", "person.go", "person_phone.go", "weird_names_table.go", "level.go")

	testutils.AssertFileContent(t, modelDir+"all_types.go", "\npackage model", allTypesModelContent)

	tableFiles, err := ioutil.ReadDir(tableDir)
	assert.NoError(t, err)

	testutils.AssertFileNamesEqual(t, tableFiles, "all_types.go", "employee.go", "link.go",
		"person.go", "person_phone.go", "weird_names_table.go")

	testutils.AssertFileContent(t, tableDir+"all_types.go", "\npackage table", allTypesTableContent)
}

var moodEnumContent = `
package enum

import "github.com/go-jet/jet/postgres"

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
package enum

import "github.com/go-jet/jet/postgres"

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
package table

import (
	"github.com/go-jet/jet/postgres"
)

var AllTypes = newAllTypesTable()

type AllTypesTable struct {
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

// creates new AllTypesTable with assigned alias
func (a *AllTypesTable) AS(alias string) *AllTypesTable {
	aliasTable := newAllTypesTable()

	aliasTable.Table.AS(alias)

	return aliasTable
}

func newAllTypesTable() *AllTypesTable {
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
	)

	return &AllTypesTable{
		Table: postgres.NewTable("test_sample", "all_types", SmallIntPtrColumn, SmallIntColumn, IntegerPtrColumn, IntegerColumn, BigIntPtrColumn, BigIntColumn, DecimalPtrColumn, DecimalColumn, NumericPtrColumn, NumericColumn, RealPtrColumn, RealColumn, DoublePrecisionPtrColumn, DoublePrecisionColumn, SmallserialColumn, SerialColumn, BigserialColumn, VarCharPtrColumn, VarCharColumn, CharPtrColumn, CharColumn, TextPtrColumn, TextColumn, ByteaPtrColumn, ByteaColumn, TimestampzPtrColumn, TimestampzColumn, TimestampPtrColumn, TimestampColumn, DatePtrColumn, DateColumn, TimezPtrColumn, TimezColumn, TimePtrColumn, TimeColumn, IntervalPtrColumn, IntervalColumn, BooleanPtrColumn, BooleanColumn, PointPtrColumn, BitPtrColumn, BitColumn, BitVaryingPtrColumn, BitVaryingColumn, TsvectorPtrColumn, TsvectorColumn, UUIDPtrColumn, UUIDColumn, XMLPtrColumn, XMLColumn, JSONPtrColumn, JSONColumn, JsonbPtrColumn, JsonbColumn, IntegerArrayPtrColumn, IntegerArrayColumn, TextArrayPtrColumn, TextArrayColumn, JsonbArrayColumn, TextMultiDimArrayPtrColumn, TextMultiDimArrayColumn),

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

		AllColumns:     postgres.ColumnList{SmallIntPtrColumn, SmallIntColumn, IntegerPtrColumn, IntegerColumn, BigIntPtrColumn, BigIntColumn, DecimalPtrColumn, DecimalColumn, NumericPtrColumn, NumericColumn, RealPtrColumn, RealColumn, DoublePrecisionPtrColumn, DoublePrecisionColumn, SmallserialColumn, SerialColumn, BigserialColumn, VarCharPtrColumn, VarCharColumn, CharPtrColumn, CharColumn, TextPtrColumn, TextColumn, ByteaPtrColumn, ByteaColumn, TimestampzPtrColumn, TimestampzColumn, TimestampPtrColumn, TimestampColumn, DatePtrColumn, DateColumn, TimezPtrColumn, TimezColumn, TimePtrColumn, TimeColumn, IntervalPtrColumn, IntervalColumn, BooleanPtrColumn, BooleanColumn, PointPtrColumn, BitPtrColumn, BitColumn, BitVaryingPtrColumn, BitVaryingColumn, TsvectorPtrColumn, TsvectorColumn, UUIDPtrColumn, UUIDColumn, XMLPtrColumn, XMLColumn, JSONPtrColumn, JSONColumn, JsonbPtrColumn, JsonbColumn, IntegerArrayPtrColumn, IntegerArrayColumn, TextArrayPtrColumn, TextArrayColumn, JsonbArrayColumn, TextMultiDimArrayPtrColumn, TextMultiDimArrayColumn},
		MutableColumns: postgres.ColumnList{SmallIntPtrColumn, SmallIntColumn, IntegerPtrColumn, IntegerColumn, BigIntPtrColumn, BigIntColumn, DecimalPtrColumn, DecimalColumn, NumericPtrColumn, NumericColumn, RealPtrColumn, RealColumn, DoublePrecisionPtrColumn, DoublePrecisionColumn, SmallserialColumn, SerialColumn, BigserialColumn, VarCharPtrColumn, VarCharColumn, CharPtrColumn, CharColumn, TextPtrColumn, TextColumn, ByteaPtrColumn, ByteaColumn, TimestampzPtrColumn, TimestampzColumn, TimestampPtrColumn, TimestampColumn, DatePtrColumn, DateColumn, TimezPtrColumn, TimezColumn, TimePtrColumn, TimeColumn, IntervalPtrColumn, IntervalColumn, BooleanPtrColumn, BooleanColumn, PointPtrColumn, BitPtrColumn, BitColumn, BitVaryingPtrColumn, BitVaryingColumn, TsvectorPtrColumn, TsvectorColumn, UUIDPtrColumn, UUIDColumn, XMLPtrColumn, XMLColumn, JSONPtrColumn, JSONColumn, JsonbPtrColumn, JsonbColumn, IntegerArrayPtrColumn, IntegerArrayColumn, TextArrayPtrColumn, TextArrayColumn, JsonbArrayColumn, TextMultiDimArrayPtrColumn, TextMultiDimArrayColumn},
	}
}
`
