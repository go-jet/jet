package mysql

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/mysql"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/testutils"
	mysql2 "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/tests/dbconfig"
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
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "with dsn",
			args: []string{
				"-dsn=mysql://" + dbconfig.MySQLConnectionString(sourceIsMariaDB(), "dvds"),
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
				"-host=" + dbconfig.MySqLHost,
				"-port=" + strconv.Itoa(dbconfig.MySQLPort),
				"-user=" + dbconfig.MySQLUser,
				"-password=" + dbconfig.MySQLPassword,
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
	DefaultColumns mysql.ColumnList
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
		defaultColumns    = mysql.ColumnList{}
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
		DefaultColumns: defaultColumns,
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
	DefaultColumns mysql.ColumnList
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
		defaultColumns   = mysql.ColumnList{LastUpdateColumn}
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
		DefaultColumns: defaultColumns,
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
	DefaultColumns mysql.ColumnList
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
		defaultColumns  = mysql.ColumnList{}
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
		DefaultColumns: defaultColumns,
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

func TestGeneratedTestSampleDatabase(t *testing.T) {

	enumDir := filepath.Join(testRoot, "/.gentestdata/mysql/test_sample/enum/")
	modelDir := filepath.Join(testRoot, "/.gentestdata/mysql/test_sample/model/")
	tableDir := filepath.Join(testRoot, "/.gentestdata/mysql/test_sample/table/")
	viewDir := filepath.Join(testRoot, "/.gentestdata/mysql/test_sample/view/")

	testutils.AssertFileNamesEqual(t, enumDir, "all_types_enum.go", "all_types_enum_ptr.go",
		"all_types_view_enum.go", "all_types_view_enum_ptr.go")
	testutils.AssertFileContent(t, enumDir+"/all_types_enum.go", allTypesEnum)

	testutils.AssertFileNamesEqual(t, modelDir, "all_types.go", "all_types_enum.go", "all_types_enum_ptr.go",
		"all_types_view.go", "all_types_view_enum.go", "all_types_view_enum_ptr.go", "link.go", "link2.go",
		"floats.go", "user.go", "people.go")

	testutils.AssertFileContent(t, modelDir+"/all_types.go", allTypesModelContent)

	testutils.AssertFileNamesEqual(t, tableDir, "all_types.go",
		"link.go", "link2.go", "user.go", "floats.go", "table_use_schema.go", "people.go")
	testutils.AssertFileContent(t, tableDir+"/all_types.go", allTypesTableContent)

	testutils.AssertFileNamesEqual(t, viewDir, "all_types_view.go", "view_use_schema.go")
}

var allTypesEnum = `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package enum

import "github.com/go-jet/jet/v2/mysql"

var AllTypesEnum = &struct {
	Value1 mysql.StringExpression
	Value2 mysql.StringExpression
	Value3 mysql.StringExpression
}{
	Value1: mysql.NewEnumValue("value1"),
	Value2: mysql.NewEnumValue("value2"),
	Value3: mysql.NewEnumValue("value3"),
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
	"time"
)

type AllTypes struct {
	ID            int32 ` + "`" + `sql:"primary_key"` + "`" + `
	Boolean       bool
	BooleanPtr    *bool
	TinyInt       int8
	UTinyInt      uint8
	SmallInt      int16
	USmallInt     uint16
	MediumInt     int32
	UMediumInt    uint32
	Integer       int32
	UInteger      uint32
	BigInt        int64
	UBigInt       uint64
	TinyIntPtr    *int8
	UTinyIntPtr   *uint8
	SmallIntPtr   *int16
	USmallIntPtr  *uint16
	MediumIntPtr  *int32
	UMediumIntPtr *uint32
	IntegerPtr    *int32
	UIntegerPtr   *uint32
	BigIntPtr     *int64
	UBigIntPtr    *uint64
	Decimal       float64
	DecimalPtr    *float64
	Numeric       float64
	NumericPtr    *float64
	Float         float64
	FloatPtr      *float64
	Double        float64
	DoublePtr     *float64
	Real          float64
	RealPtr       *float64
	Bit           string
	BitPtr        *string
	Time          time.Time
	TimePtr       *time.Time
	Date          time.Time
	DatePtr       *time.Time
	DateTime      time.Time
	DateTimePtr   *time.Time
	Timestamp     time.Time
	TimestampPtr  *time.Time
	Year          int16
	YearPtr       *int16
	Char          string
	CharPtr       *string
	VarChar       string
	VarCharPtr    *string
	Binary        []byte
	BinaryPtr     *[]byte
	VarBinary     []byte
	VarBinaryPtr  *[]byte
	Blob          []byte
	BlobPtr       *[]byte
	Text          string
	TextPtr       *string
	Enum          AllTypesEnum
	EnumPtr       *AllTypesEnumPtr
	Set           string
	SetPtr        *string
	JSON          string
	JSONPtr       *string
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
	"github.com/go-jet/jet/v2/mysql"
)

var AllTypes = newAllTypesTable("test_sample", "all_types", "")

type allTypesTable struct {
	mysql.Table

	// Columns
	ID            mysql.ColumnInteger
	Boolean       mysql.ColumnBool
	BooleanPtr    mysql.ColumnBool
	TinyInt       mysql.ColumnInteger
	UTinyInt      mysql.ColumnInteger
	SmallInt      mysql.ColumnInteger
	USmallInt     mysql.ColumnInteger
	MediumInt     mysql.ColumnInteger
	UMediumInt    mysql.ColumnInteger
	Integer       mysql.ColumnInteger
	UInteger      mysql.ColumnInteger
	BigInt        mysql.ColumnInteger
	UBigInt       mysql.ColumnInteger
	TinyIntPtr    mysql.ColumnInteger
	UTinyIntPtr   mysql.ColumnInteger
	SmallIntPtr   mysql.ColumnInteger
	USmallIntPtr  mysql.ColumnInteger
	MediumIntPtr  mysql.ColumnInteger
	UMediumIntPtr mysql.ColumnInteger
	IntegerPtr    mysql.ColumnInteger
	UIntegerPtr   mysql.ColumnInteger
	BigIntPtr     mysql.ColumnInteger
	UBigIntPtr    mysql.ColumnInteger
	Decimal       mysql.ColumnFloat
	DecimalPtr    mysql.ColumnFloat
	Numeric       mysql.ColumnFloat
	NumericPtr    mysql.ColumnFloat
	Float         mysql.ColumnFloat
	FloatPtr      mysql.ColumnFloat
	Double        mysql.ColumnFloat
	DoublePtr     mysql.ColumnFloat
	Real          mysql.ColumnFloat
	RealPtr       mysql.ColumnFloat
	Bit           mysql.ColumnString
	BitPtr        mysql.ColumnString
	Time          mysql.ColumnTime
	TimePtr       mysql.ColumnTime
	Date          mysql.ColumnDate
	DatePtr       mysql.ColumnDate
	DateTime      mysql.ColumnTimestamp
	DateTimePtr   mysql.ColumnTimestamp
	Timestamp     mysql.ColumnTimestamp
	TimestampPtr  mysql.ColumnTimestamp
	Year          mysql.ColumnInteger
	YearPtr       mysql.ColumnInteger
	Char          mysql.ColumnString
	CharPtr       mysql.ColumnString
	VarChar       mysql.ColumnString
	VarCharPtr    mysql.ColumnString
	Binary        mysql.ColumnBlob
	BinaryPtr     mysql.ColumnBlob
	VarBinary     mysql.ColumnBlob
	VarBinaryPtr  mysql.ColumnBlob
	Blob          mysql.ColumnBlob
	BlobPtr       mysql.ColumnBlob
	Text          mysql.ColumnString
	TextPtr       mysql.ColumnString
	Enum          mysql.ColumnString
	EnumPtr       mysql.ColumnString
	Set           mysql.ColumnString
	SetPtr        mysql.ColumnString
	JSON          mysql.ColumnString
	JSONPtr       mysql.ColumnString

	AllColumns     mysql.ColumnList
	MutableColumns mysql.ColumnList
	DefaultColumns mysql.ColumnList
}

type AllTypesTable struct {
	allTypesTable

	NEW allTypesTable
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
		NEW:           newAllTypesTableImpl("", "new", ""),
	}
}

func newAllTypesTableImpl(schemaName, tableName, alias string) allTypesTable {
	var (
		IDColumn            = mysql.IntegerColumn("id")
		BooleanColumn       = mysql.BoolColumn("boolean")
		BooleanPtrColumn    = mysql.BoolColumn("boolean_ptr")
		TinyIntColumn       = mysql.IntegerColumn("tiny_int")
		UTinyIntColumn      = mysql.IntegerColumn("u_tiny_int")
		SmallIntColumn      = mysql.IntegerColumn("small_int")
		USmallIntColumn     = mysql.IntegerColumn("u_small_int")
		MediumIntColumn     = mysql.IntegerColumn("medium_int")
		UMediumIntColumn    = mysql.IntegerColumn("u_medium_int")
		IntegerColumn       = mysql.IntegerColumn("integer")
		UIntegerColumn      = mysql.IntegerColumn("u_integer")
		BigIntColumn        = mysql.IntegerColumn("big_int")
		UBigIntColumn       = mysql.IntegerColumn("u_big_int")
		TinyIntPtrColumn    = mysql.IntegerColumn("tiny_int_ptr")
		UTinyIntPtrColumn   = mysql.IntegerColumn("u_tiny_int_ptr")
		SmallIntPtrColumn   = mysql.IntegerColumn("small_int_ptr")
		USmallIntPtrColumn  = mysql.IntegerColumn("u_small_int_ptr")
		MediumIntPtrColumn  = mysql.IntegerColumn("medium_int_ptr")
		UMediumIntPtrColumn = mysql.IntegerColumn("u_medium_int_ptr")
		IntegerPtrColumn    = mysql.IntegerColumn("integer_ptr")
		UIntegerPtrColumn   = mysql.IntegerColumn("u_integer_ptr")
		BigIntPtrColumn     = mysql.IntegerColumn("big_int_ptr")
		UBigIntPtrColumn    = mysql.IntegerColumn("u_big_int_ptr")
		DecimalColumn       = mysql.FloatColumn("decimal")
		DecimalPtrColumn    = mysql.FloatColumn("decimal_ptr")
		NumericColumn       = mysql.FloatColumn("numeric")
		NumericPtrColumn    = mysql.FloatColumn("numeric_ptr")
		FloatColumn         = mysql.FloatColumn("float")
		FloatPtrColumn      = mysql.FloatColumn("float_ptr")
		DoubleColumn        = mysql.FloatColumn("double")
		DoublePtrColumn     = mysql.FloatColumn("double_ptr")
		RealColumn          = mysql.FloatColumn("real")
		RealPtrColumn       = mysql.FloatColumn("real_ptr")
		BitColumn           = mysql.StringColumn("bit")
		BitPtrColumn        = mysql.StringColumn("bit_ptr")
		TimeColumn          = mysql.TimeColumn("time")
		TimePtrColumn       = mysql.TimeColumn("time_ptr")
		DateColumn          = mysql.DateColumn("date")
		DatePtrColumn       = mysql.DateColumn("date_ptr")
		DateTimeColumn      = mysql.TimestampColumn("date_time")
		DateTimePtrColumn   = mysql.TimestampColumn("date_time_ptr")
		TimestampColumn     = mysql.TimestampColumn("timestamp")
		TimestampPtrColumn  = mysql.TimestampColumn("timestamp_ptr")
		YearColumn          = mysql.IntegerColumn("year")
		YearPtrColumn       = mysql.IntegerColumn("year_ptr")
		CharColumn          = mysql.StringColumn("char")
		CharPtrColumn       = mysql.StringColumn("char_ptr")
		VarCharColumn       = mysql.StringColumn("var_char")
		VarCharPtrColumn    = mysql.StringColumn("var_char_ptr")
		BinaryColumn        = mysql.BlobColumn("binary")
		BinaryPtrColumn     = mysql.BlobColumn("binary_ptr")
		VarBinaryColumn     = mysql.BlobColumn("var_binary")
		VarBinaryPtrColumn  = mysql.BlobColumn("var_binary_ptr")
		BlobColumn          = mysql.BlobColumn("blob")
		BlobPtrColumn       = mysql.BlobColumn("blob_ptr")
		TextColumn          = mysql.StringColumn("text")
		TextPtrColumn       = mysql.StringColumn("text_ptr")
		EnumColumn          = mysql.StringColumn("enum")
		EnumPtrColumn       = mysql.StringColumn("enum_ptr")
		SetColumn           = mysql.StringColumn("set")
		SetPtrColumn        = mysql.StringColumn("set_ptr")
		JSONColumn          = mysql.StringColumn("json")
		JSONPtrColumn       = mysql.StringColumn("json_ptr")
		allColumns          = mysql.ColumnList{IDColumn, BooleanColumn, BooleanPtrColumn, TinyIntColumn, UTinyIntColumn, SmallIntColumn, USmallIntColumn, MediumIntColumn, UMediumIntColumn, IntegerColumn, UIntegerColumn, BigIntColumn, UBigIntColumn, TinyIntPtrColumn, UTinyIntPtrColumn, SmallIntPtrColumn, USmallIntPtrColumn, MediumIntPtrColumn, UMediumIntPtrColumn, IntegerPtrColumn, UIntegerPtrColumn, BigIntPtrColumn, UBigIntPtrColumn, DecimalColumn, DecimalPtrColumn, NumericColumn, NumericPtrColumn, FloatColumn, FloatPtrColumn, DoubleColumn, DoublePtrColumn, RealColumn, RealPtrColumn, BitColumn, BitPtrColumn, TimeColumn, TimePtrColumn, DateColumn, DatePtrColumn, DateTimeColumn, DateTimePtrColumn, TimestampColumn, TimestampPtrColumn, YearColumn, YearPtrColumn, CharColumn, CharPtrColumn, VarCharColumn, VarCharPtrColumn, BinaryColumn, BinaryPtrColumn, VarBinaryColumn, VarBinaryPtrColumn, BlobColumn, BlobPtrColumn, TextColumn, TextPtrColumn, EnumColumn, EnumPtrColumn, SetColumn, SetPtrColumn, JSONColumn, JSONPtrColumn}
		mutableColumns      = mysql.ColumnList{BooleanColumn, BooleanPtrColumn, TinyIntColumn, UTinyIntColumn, SmallIntColumn, USmallIntColumn, MediumIntColumn, UMediumIntColumn, IntegerColumn, UIntegerColumn, BigIntColumn, UBigIntColumn, TinyIntPtrColumn, UTinyIntPtrColumn, SmallIntPtrColumn, USmallIntPtrColumn, MediumIntPtrColumn, UMediumIntPtrColumn, IntegerPtrColumn, UIntegerPtrColumn, BigIntPtrColumn, UBigIntPtrColumn, DecimalColumn, DecimalPtrColumn, NumericColumn, NumericPtrColumn, FloatColumn, FloatPtrColumn, DoubleColumn, DoublePtrColumn, RealColumn, RealPtrColumn, BitColumn, BitPtrColumn, TimeColumn, TimePtrColumn, DateColumn, DatePtrColumn, DateTimeColumn, DateTimePtrColumn, TimestampColumn, TimestampPtrColumn, YearColumn, YearPtrColumn, CharColumn, CharPtrColumn, VarCharColumn, VarCharPtrColumn, BinaryColumn, BinaryPtrColumn, VarBinaryColumn, VarBinaryPtrColumn, BlobColumn, BlobPtrColumn, TextColumn, TextPtrColumn, EnumColumn, EnumPtrColumn, SetColumn, SetPtrColumn, JSONColumn, JSONPtrColumn}
		defaultColumns      = mysql.ColumnList{BooleanColumn, TinyIntColumn, UTinyIntColumn, SmallIntColumn, USmallIntColumn, MediumIntColumn, UMediumIntColumn, IntegerColumn, UIntegerColumn, BigIntColumn, UBigIntColumn, DecimalColumn, NumericColumn, FloatColumn, DoubleColumn, RealColumn, BitColumn, TimeColumn, DateColumn, DateTimeColumn, TimestampColumn, YearColumn, CharColumn, VarCharColumn, BinaryColumn, VarBinaryColumn, EnumColumn, SetColumn}
	)

	return allTypesTable{
		Table: mysql.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		ID:            IDColumn,
		Boolean:       BooleanColumn,
		BooleanPtr:    BooleanPtrColumn,
		TinyInt:       TinyIntColumn,
		UTinyInt:      UTinyIntColumn,
		SmallInt:      SmallIntColumn,
		USmallInt:     USmallIntColumn,
		MediumInt:     MediumIntColumn,
		UMediumInt:    UMediumIntColumn,
		Integer:       IntegerColumn,
		UInteger:      UIntegerColumn,
		BigInt:        BigIntColumn,
		UBigInt:       UBigIntColumn,
		TinyIntPtr:    TinyIntPtrColumn,
		UTinyIntPtr:   UTinyIntPtrColumn,
		SmallIntPtr:   SmallIntPtrColumn,
		USmallIntPtr:  USmallIntPtrColumn,
		MediumIntPtr:  MediumIntPtrColumn,
		UMediumIntPtr: UMediumIntPtrColumn,
		IntegerPtr:    IntegerPtrColumn,
		UIntegerPtr:   UIntegerPtrColumn,
		BigIntPtr:     BigIntPtrColumn,
		UBigIntPtr:    UBigIntPtrColumn,
		Decimal:       DecimalColumn,
		DecimalPtr:    DecimalPtrColumn,
		Numeric:       NumericColumn,
		NumericPtr:    NumericPtrColumn,
		Float:         FloatColumn,
		FloatPtr:      FloatPtrColumn,
		Double:        DoubleColumn,
		DoublePtr:     DoublePtrColumn,
		Real:          RealColumn,
		RealPtr:       RealPtrColumn,
		Bit:           BitColumn,
		BitPtr:        BitPtrColumn,
		Time:          TimeColumn,
		TimePtr:       TimePtrColumn,
		Date:          DateColumn,
		DatePtr:       DatePtrColumn,
		DateTime:      DateTimeColumn,
		DateTimePtr:   DateTimePtrColumn,
		Timestamp:     TimestampColumn,
		TimestampPtr:  TimestampPtrColumn,
		Year:          YearColumn,
		YearPtr:       YearPtrColumn,
		Char:          CharColumn,
		CharPtr:       CharPtrColumn,
		VarChar:       VarCharColumn,
		VarCharPtr:    VarCharPtrColumn,
		Binary:        BinaryColumn,
		BinaryPtr:     BinaryPtrColumn,
		VarBinary:     VarBinaryColumn,
		VarBinaryPtr:  VarBinaryPtrColumn,
		Blob:          BlobColumn,
		BlobPtr:       BlobPtrColumn,
		Text:          TextColumn,
		TextPtr:       TextPtrColumn,
		Enum:          EnumColumn,
		EnumPtr:       EnumPtrColumn,
		Set:           SetColumn,
		SetPtr:        SetPtrColumn,
		JSON:          JSONColumn,
		JSONPtr:       JSONPtrColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
		DefaultColumns: defaultColumns,
	}
}
`

func TestAllowTablesViewsEnums(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "with dsn",
			args: []string{
				"-dsn=mysql://" + dbconfig.MySQLConnectionString(sourceIsMariaDB(), "dvds"),
				"-tables=actor,ADDRESS,Category, city ,country,staff,store,rental",
				"-views=actor_info,CUSTomER_LIST, film_list",
				"-enums=film_list_rating,film_rating",
				"-path=" + genTestDir3,
			},
		},
		{
			name: "without dsn",
			args: []string{
				"-source=MySQL",
				"-dbname=dvds",
				"-host=" + dbconfig.MySqLHost,
				"-port=" + strconv.Itoa(dbconfig.MySQLPort),
				"-user=" + dbconfig.MySQLUser,
				"-password=" + dbconfig.MySQLPassword,
				"-tables=actor,ADDRESS,Category, city ,country,staff,store,rental",
				"-views=actor_info,CUSTomER_LIST, film_list",
				"-enums=film_list_rating,film_rating",
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

			testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/table", "category.go", "actor.go", "address.go",
				"city.go", "country.go", "staff.go", "store.go", "rental.go", "table_use_schema.go")

			testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/view", "actor_info.go", "customer_list.go",
				"film_list.go", "view_use_schema.go")

			testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/enum", "film_list_rating.go", "film_rating.go")

			testutils.AssertFileNamesEqual(t, genTestDir3+"/dvds/model", "category.go", "actor.go", "address.go",
				"city.go", "country.go", "staff.go", "store.go", "rental.go", "actor_info.go",
				"customer_list.go", "film_list.go", "film_list_rating.go", "film_rating.go")
		})
	}
}

func TestAllowAndIgnoreTablesViewsEnums(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "with dsn",
			args: []string{
				"-dsn=mysql://" + dbconfig.MySQLConnectionString(sourceIsMariaDB(), "dvds"),
				"-tables=actor,ADDRESS,Category, city ,country,staff,store,rental",
				"-views=actor_info,CUSTomER_LIST, film_list",
				"-enums=film_list_rating,film_rating",
				"-ignore-tables=actor,ADDRESS,store,rental",
				"-ignore-views=film_list",
				"-ignore-enums=film_rating",
				"-path=" + genTestDir3,
			},
		},
		{
			name: "without dsn",
			args: []string{
				"-source=MySQL",
				"-dbname=dvds",
				"-host=" + dbconfig.MySqLHost,
				"-port=" + strconv.Itoa(dbconfig.MySQLPort),
				"-user=" + dbconfig.MySQLUser,
				"-password=" + dbconfig.MySQLPassword,
				"-tables=actor,ADDRESS,Category, city ,country,staff,store,rental",
				"-views=actor_info,CUSTomER_LIST, film_list",
				"-enums=film_list_rating,film_rating",
				"-ignore-tables=actor,ADDRESS,store,rental",
				"-ignore-views=film_list",
				"-ignore-enums=film_rating",
				"-path=" + genTestDir3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command("jet", tt.args...)

			var stdOut bytes.Buffer
			cmd.Stderr = os.Stderr
			cmd.Stdout = &stdOut

			err := cmd.Run()
			require.Error(t, err)
			require.Equal(t, "exit status 1", err.Error())

			stdOutput := stdOut.String()
			require.Contains(t, stdOutput, "ERROR: cannot use both -tables and -ignore-tables flags simultaneously. Please specify only one option.")
		})
	}
}
