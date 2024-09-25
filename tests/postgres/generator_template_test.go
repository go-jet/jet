package postgres

import (
	"database/sql"
	"fmt"
	"path"
	"testing"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/postgres"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/3rdparty/snaker"
	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/internal/utils/dbidentifier"
	postgres2 "github.com/go-jet/jet/v2/postgres"
	file2 "github.com/go-jet/jet/v2/tests/internal/utils/file"
	"github.com/stretchr/testify/require"
)

const tempTestDir = "./.tempTestDir"

var defaultModelPath = path.Join(tempTestDir, "jetdb/dvds/model")
var defaultSqlBuilderPath = path.Join(tempTestDir, "jetdb/dvds/table")
var defaultActorModelFilePath = path.Join(tempTestDir, "jetdb/dvds/model", "actor.go")
var defaultTableSQLBuilderFilePath = path.Join(tempTestDir, "jetdb/dvds/table")
var defaultViewSQLBuilderFilePath = path.Join(tempTestDir, "jetdb/dvds/view")
var defaultEnumSQLBuilderFilePath = path.Join(tempTestDir, "jetdb/dvds/enum")
var defaultActorSQLBuilderFilePath = path.Join(tempTestDir, "jetdb/dvds/table", "actor.go")

var dbConnection = postgres.DBConnection{
	Host:       PgHost,
	Port:       PgPort,
	User:       PgUser,
	Password:   PgPassword,
	DBName:     PgDBName,
	SchemaName: "dvds",
	SslMode:    "disable",
}

func getDbConnection() postgres.DBConnection {
	if sourceIsCockroachDB() {
		dbConnection.Host = CockroachHost
		dbConnection.Port = CockroachPort
	} else {
		dbConnection.Host = PgHost
		dbConnection.Port = PgPort
	}

	return dbConnection
}

func TestGeneratorTemplate_Schema_ChangePath(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).UsePath("new/schema/path")
			}),
	)

	require.Nil(t, err)

	file2.Exists(t, tempTestDir, "jetdb/new/schema/path/model/actor.go")
	file2.Exists(t, tempTestDir, "jetdb/new/schema/path/table/actor.go")
	file2.Exists(t, tempTestDir, "jetdb/new/schema/path/view/actor_info.go")
	file2.Exists(t, tempTestDir, "jetdb/new/schema/path/enum/mpaa_rating.go")
}

func TestGeneratorTemplate_Model_SkipGeneration(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseModel(template.Model{
						Skip: true,
					})
			}),
	)

	require.Nil(t, err)

	file2.NotExists(t, defaultActorModelFilePath)
}

func TestGeneratorTemplate_SQLBuilder_SkipGeneration(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseSQLBuilder(template.SQLBuilder{
						Skip: true,
					})
			}),
	)

	require.Nil(t, err)

	file2.NotExists(t, defaultTableSQLBuilderFilePath, "actor.go")
	file2.NotExists(t, defaultViewSQLBuilderFilePath, "actor_info.go")
	file2.NotExists(t, defaultEnumSQLBuilderFilePath, "mpaa_rating.go")
}

func TestGeneratorTemplate_Model_ChangePath(t *testing.T) {
	const newModelPath = "/new/model/path"

	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseModel(template.DefaultModel().UsePath(newModelPath))
			}),
	)
	require.Nil(t, err)

	file2.Exists(t, tempTestDir, "jetdb", "dvds", newModelPath, "actor.go")
	file2.NotExists(t, defaultActorModelFilePath)
}

func TestGeneratorTemplate_SQLBuilder_ChangePath(t *testing.T) {
	const newModelPath = "/new/sql-builder/path"

	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseSQLBuilder(template.DefaultSQLBuilder().UsePath(newModelPath))
			}),
	)
	require.Nil(t, err)

	file2.Exists(t, tempTestDir, "jetdb", "dvds", newModelPath, "table", "actor.go")
	file2.Exists(t, tempTestDir, "jetdb", "dvds", newModelPath, "view", "actor_info.go")
	file2.Exists(t, tempTestDir, "jetdb", "dvds", newModelPath, "enum", "mpaa_rating.go")

	file2.NotExists(t, defaultTableSQLBuilderFilePath, "actor.go")
	file2.NotExists(t, defaultViewSQLBuilderFilePath, "actor_info.go")
	file2.NotExists(t, defaultEnumSQLBuilderFilePath, "mpaa_rating.go")
}

func TestGeneratorTemplate_Model_RenameFilesAndTypes(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseModel(template.DefaultModel().
						UseTable(func(table metadata.Table) template.TableModel {
							return template.DefaultTableModel(table).
								UseFileName(schemaMetaData.Name + "_" + table.Name).
								UseTypeName(dbidentifier.ToGoIdentifier(table.Name) + "Table")
						}).
						UseView(func(table metadata.Table) template.ViewModel {
							return template.DefaultViewModel(table).
								UseFileName(schemaMetaData.Name + "_" + table.Name + "_view").
								UseTypeName(dbidentifier.ToGoIdentifier(table.Name) + "View")
						}).
						UseEnum(func(enumMetaData metadata.Enum) template.EnumModel {
							return template.DefaultEnumModel(enumMetaData).
								UseFileName(enumMetaData.Name + "_enum").
								UseTypeName(dbidentifier.ToGoIdentifier(enumMetaData.Name) + "Enum")
						}),
					)
			}),
	)
	require.Nil(t, err)

	actor := file2.Exists(t, defaultModelPath, "dvds_actor.go")
	require.Contains(t, actor, "type ActorTable struct {")

	actorInfo := file2.Exists(t, defaultModelPath, "dvds_actor_info_view.go")
	require.Contains(t, actorInfo, "type ActorInfoView struct {")

	mpaaRating := file2.Exists(t, defaultModelPath, "mpaa_rating_enum.go")
	require.Contains(t, mpaaRating, "type MpaaRatingEnum string")
	require.Contains(t, mpaaRating, "MpaaRatingEnumAllValues")
}

func TestGeneratorTemplate_Model_SkipTableAndEnum(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseModel(template.DefaultModel().
						UseTable(func(table metadata.Table) template.TableModel {
							return template.TableModel{
								Skip: true,
							}
						}).
						UseEnum(func(enumMetaData metadata.Enum) template.EnumModel {
							return template.EnumModel{
								Skip: true,
							}
						}),
					)
			}),
	)
	require.Nil(t, err)

	file2.NotExists(t, defaultModelPath, "actor.go")
	file2.Exists(t, defaultModelPath, "actor_info.go")
	file2.NotExists(t, defaultModelPath, "mpaa_rating.go")
}

func TestGeneratorTemplate_SQLBuilder_SkipTableAndEnum(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseSQLBuilder(template.DefaultSQLBuilder().
						UseTable(func(table metadata.Table) template.TableSQLBuilder {
							if table.Name != "city" {
								return template.TableSQLBuilder{Skip: true}
							}

							return template.DefaultTableSQLBuilder(table)
						}).
						UseView(func(view metadata.Table) template.ViewSQLBuilder {
							if view.Name != "film_list" {
								return template.TableSQLBuilder{Skip: true}
							}

							return template.DefaultViewSQLBuilder(view)
						}).
						UseEnum(func(enumMetaData metadata.Enum) template.EnumSQLBuilder {
							return template.EnumSQLBuilder{Skip: true}
						}),
					)
			}),
	)
	require.Nil(t, err)

	testutils.AssertFileNamesEqual(t, defaultTableSQLBuilderFilePath, "city.go", "table_use_schema.go")
	testutils.AssertFileNamesEqual(t, defaultViewSQLBuilderFilePath, "film_list.go", "view_use_schema.go")
	file2.NotExists(t, defaultEnumSQLBuilderFilePath, "mpaa_rating.go")

	testutils.AssertFileContent(t, defaultTableSQLBuilderFilePath+"/table_use_schema.go", `
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
	City = City.FromSchema(schema)
}
`)

	testutils.AssertFileContent(t, defaultViewSQLBuilderFilePath+"/view_use_schema.go", `
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
	FilmList = FilmList.FromSchema(schema)
}
`)
}

func TestGeneratorTemplate_SQLBuilder_ChangeTypeAndFileName(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseSQLBuilder(template.DefaultSQLBuilder().
						UseTable(func(table metadata.Table) template.TableSQLBuilder {
							return template.DefaultTableSQLBuilder(table).
								UseFileName(schemaMetaData.Name + "_" + table.Name + "_table").
								UseTypeName(dbidentifier.ToGoIdentifier(table.Name) + "TableSQLBuilder").
								UseInstanceName("T_" + dbidentifier.ToGoIdentifier(table.Name))
						}).
						UseView(func(table metadata.Table) template.ViewSQLBuilder {
							return template.DefaultViewSQLBuilder(table).
								UseFileName(schemaMetaData.Name + "_" + table.Name + "_view").
								UseTypeName(dbidentifier.ToGoIdentifier(table.Name) + "ViewSQLBuilder").
								UseInstanceName("V_" + dbidentifier.ToGoIdentifier(table.Name))
						}).
						UseEnum(func(enum metadata.Enum) template.EnumSQLBuilder {
							return template.DefaultEnumSQLBuilder(enum).
								UseFileName(schemaMetaData.Name + "_" + enum.Name + "_enum").
								UseInstanceName(dbidentifier.ToGoIdentifier(enum.Name) + "EnumSQLBuilder")
						}),
					)
			}),
	)
	require.Nil(t, err)

	actor := file2.Exists(t, defaultTableSQLBuilderFilePath, "dvds_actor_table.go")
	require.Contains(t, actor, "type ActorTableSQLBuilder struct {")
	require.Contains(t, actor, "var T_Actor = newActorTableSQLBuilder(\"dvds\", \"actor\", \"\")")
	actorInfo := file2.Exists(t, defaultViewSQLBuilderFilePath, "dvds_actor_info_view.go")
	require.Contains(t, actorInfo, "type ActorInfoViewSQLBuilder struct {")
	require.Contains(t, actorInfo, "var V_ActorInfo = newActorInfoViewSQLBuilder(\"dvds\", \"actor_info\", \"\")")
	mpaaRating := file2.Exists(t, defaultEnumSQLBuilderFilePath, "dvds_mpaa_rating_enum.go")
	require.Contains(t, mpaaRating, "var MpaaRatingEnumSQLBuilder = &struct {")

	testutils.AssertFileContent(t, defaultTableSQLBuilderFilePath+"/table_use_schema.go", `
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
	T_Actor = T_Actor.FromSchema(schema)
	T_Address = T_Address.FromSchema(schema)
	T_Category = T_Category.FromSchema(schema)
	T_City = T_City.FromSchema(schema)
	T_Country = T_Country.FromSchema(schema)
	T_Customer = T_Customer.FromSchema(schema)
	T_Film = T_Film.FromSchema(schema)
	T_FilmActor = T_FilmActor.FromSchema(schema)
	T_FilmCategory = T_FilmCategory.FromSchema(schema)
	T_Inventory = T_Inventory.FromSchema(schema)
	T_Language = T_Language.FromSchema(schema)
	T_Payment = T_Payment.FromSchema(schema)
	T_Rental = T_Rental.FromSchema(schema)
	T_Staff = T_Staff.FromSchema(schema)
	T_Store = T_Store.FromSchema(schema)
}
`)
}

func TestGeneratorTemplate_SQLBuilder_DefaultAlias(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseSQLBuilder(template.DefaultSQLBuilder().
						UseTable(func(table metadata.Table) template.TableSQLBuilder {
							if table.Name == "actor" {
								return template.DefaultTableSQLBuilder(table).UseDefaultAlias("actors")
							}
							return template.DefaultTableSQLBuilder(table)
						}),
					)
			}),
	)
	require.Nil(t, err)

	actor := file2.Exists(t, defaultTableSQLBuilderFilePath, "actor.go")
	require.Contains(t, actor, "var Actor = newActorTable(\"dvds\", \"actor\", \"actors\")")
}

func TestGeneratorTemplate_Model_AddTags(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseModel(template.DefaultModel().
						UseTable(func(table metadata.Table) template.TableModel {
							return template.DefaultTableModel(table).
								UseField(func(columnMetaData metadata.Column) template.TableModelField {
									defaultTableModelField := template.DefaultTableModelField(columnMetaData)
									return defaultTableModelField.UseTags(
										fmt.Sprintf(`json:"%s"`, snaker.SnakeToCamel(columnMetaData.Name, false)),
										fmt.Sprintf(`xml:"%s"`, columnMetaData.Name),
									)
								})
						}).
						UseView(func(table metadata.Table) template.ViewModel {
							return template.DefaultViewModel(table).
								UseField(func(columnMetaData metadata.Column) template.TableModelField {
									defaultTableModelField := template.DefaultTableModelField(columnMetaData)
									if table.Name == "actor_info" && columnMetaData.Name == "actor_id" {
										return defaultTableModelField.UseTags(`sql:"primary_key"`)
									}
									return defaultTableModelField
								})
						}),
					)
			}),
	)
	require.Nil(t, err)

	actor := file2.Exists(t, defaultActorModelFilePath)
	require.Contains(t, actor, "ActorID    int32     `sql:\"primary_key\" json:\"actorID\" xml:\"actor_id\"`")
	require.Contains(t, actor, "FirstName  string    `json:\"firstName\" xml:\"first_name\"`")

	actorInfo := file2.Exists(t, defaultModelPath, "actor_info.go")
	require.Contains(t, actorInfo, "ActorID   *int32 `sql:\"primary_key\"`")
}

func TestGeneratorTemplate_Model_ChangeFieldTypes(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseModel(template.DefaultModel().
						UseTable(func(table metadata.Table) template.TableModel {
							return template.DefaultTableModel(table).
								UseField(func(columnMetaData metadata.Column) template.TableModelField {
									defaultTableModelField := template.DefaultTableModelField(columnMetaData)

									switch defaultTableModelField.Type.Name {
									case "*string":
										defaultTableModelField.Type = template.NewType(sql.NullString{})
									case "*int32":
										defaultTableModelField.Type = template.NewType(sql.NullInt32{})
									case "*int64":
										defaultTableModelField.Type = template.NewType(sql.NullInt64{})
									case "*bool":
										defaultTableModelField.Type = template.NewType(sql.NullBool{})
									case "*float64":
										defaultTableModelField.Type = template.NewType(sql.NullFloat64{})
									case "*time.Time":
										defaultTableModelField.Type = template.NewType(sql.NullTime{})
									}
									return defaultTableModelField
								})
						}),
					)
			}),
	)

	require.Nil(t, err)

	data := file2.Exists(t, defaultModelPath, "film.go")
	require.Contains(t, data, "\"database/sql\"")
	require.Contains(t, data, "Description     sql.NullString")
	require.Contains(t, data, "ReleaseYear     sql.NullInt32")
	require.Contains(t, data, "SpecialFeatures sql.NullString")
}

func TestGeneratorTemplate_SQLBuilder_ChangeColumnTypes(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseSQLBuilder(template.DefaultSQLBuilder().
						UseTable(func(table metadata.Table) template.TableSQLBuilder {
							return template.DefaultTableSQLBuilder(table).
								UseColumn(func(column metadata.Column) template.TableSQLBuilderColumn {
									defaultColumn := template.DefaultTableSQLBuilderColumn(column)

									if defaultColumn.Name == "ActorID" {
										defaultColumn.Type = "String"
									}

									return defaultColumn
								})
						}),
					)
			}),
	)

	require.Nil(t, err)

	actor := file2.Exists(t, defaultActorSQLBuilderFilePath)
	require.Contains(t, actor, "ActorID    postgres.ColumnString")
}

func TestRenameEnumValueName(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseSQLBuilder(template.DefaultSQLBuilder().
						UseEnum(func(enum metadata.Enum) template.EnumSQLBuilder {
							defaultEnumSqlBuilder := template.DefaultEnumSQLBuilder(enum)

							defaultValueNameFunc := defaultEnumSqlBuilder.ValueName

							defaultEnumSqlBuilder.ValueName = func(enumValue string) string {
								if enumValue == "G" {
									return "GRating"
								}
								return defaultValueNameFunc(enumValue)
							}

							return defaultEnumSqlBuilder
						}),
					)
			}),
	)
	require.NoError(t, err)
	testutils.AssertFileContent(t, defaultEnumSQLBuilderFilePath+"/mpaa_rating.go", `
//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package enum

import "github.com/go-jet/jet/v2/postgres"

var MpaaRating = &struct {
	GRating postgres.StringExpression
	Pg      postgres.StringExpression
	Pg13    postgres.StringExpression
	R       postgres.StringExpression
	Nc17    postgres.StringExpression
}{
	GRating: postgres.NewEnumValue("G"),
	Pg:      postgres.NewEnumValue("PG"),
	Pg13:    postgres.NewEnumValue("PG-13"),
	R:       postgres.NewEnumValue("R"),
	Nc17:    postgres.NewEnumValue("NC-17"),
}
`)
}

func TestGeneratorTemplate_Model_SqlBuilder_RenameStructFieldNames(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		getDbConnection(),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseModel(template.DefaultModel().
						UseTable(func(table metadata.Table) template.TableModel {
							return template.DefaultTableModel(table).
								UseField(func(column metadata.Column) template.TableModelField {
									defaultTableModelField := template.DefaultTableModelField(column)

									if table.Name == "payment" && column.Name == "amount" {
										defaultTableModelField.Name = "AmountInCents"
									}
									return defaultTableModelField
								})
						}),
					).
					UseSQLBuilder(template.DefaultSQLBuilder().
						UseTable(func(table metadata.Table) template.TableSQLBuilder {
							return template.DefaultTableSQLBuilder(table).
								UseColumn(func(column metadata.Column) template.TableSQLBuilderColumn {
									defaultSqlBuilderColumn := template.DefaultTableSQLBuilderColumn(column)

									if table.Name == "payment" && column.Name == "amount" {
										defaultSqlBuilderColumn.Name = "AmountInCents"
									}

									return defaultSqlBuilderColumn
								})
						}),
					)
			}),
	)
	require.NoError(t, err)

	filmModelData := file2.Exists(t, defaultModelPath, "payment.go")
	require.Contains(t, filmModelData, "AmountInCents float64")
	filmSqlBuilderData := file2.Exists(t, defaultSqlBuilderPath, "payment.go")
	require.Contains(t, filmSqlBuilderData, "AmountInCents postgres.ColumnFloat")
	require.Contains(t, filmSqlBuilderData, "AmountInCentsColumn = postgres.FloatColumn(\"amount\")")
	require.Contains(t, filmSqlBuilderData, "allColumns          = postgres.ColumnList{PaymentIDColumn, CustomerIDColumn, StaffIDColumn, RentalIDColumn, AmountInCentsColumn, PaymentDateColumn}")
	require.Contains(t, filmSqlBuilderData, "AmountInCents: AmountInCentsColumn,")
}
