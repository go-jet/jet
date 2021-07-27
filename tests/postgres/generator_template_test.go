package postgres

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/postgres"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/3rdparty/snaker"
	"github.com/go-jet/jet/v2/internal/utils"
	postgres2 "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/dbconfig"
	file2 "github.com/go-jet/jet/v2/tests/internal/utils/file"
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

const tempTestDir = "./.tempTestDir"

var defaultModelPath = path.Join(tempTestDir, "jetdb/dvds/model")
var defaultActorModelFilePath = path.Join(tempTestDir, "jetdb/dvds/model", "actor.go")
var defaultTableSQLBuilderFilePath = path.Join(tempTestDir, "jetdb/dvds/table")
var defaultViewSQLBuilderFilePath = path.Join(tempTestDir, "jetdb/dvds/view")
var defaultEnumSQLBuilderFilePath = path.Join(tempTestDir, "jetdb/dvds/enum")
var defaultActorSQLBuilderFilePath = path.Join(tempTestDir, "jetdb/dvds/table", "actor.go")

var dbConnection = postgres.DBConnection{
	Host:       dbconfig.PgHost,
	Port:       5432,
	User:       dbconfig.PgUser,
	Password:   dbconfig.PgPassword,
	DBName:     dbconfig.PgDBName,
	SchemaName: "dvds",
	SslMode:    "disable",
}

func TestGeneratorTemplate_Schema_ChangePath(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		dbConnection,
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
		dbConnection,
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
		dbConnection,
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
		dbConnection,
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
		dbConnection,
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
		dbConnection,
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseModel(template.DefaultModel().
						UseTable(func(table metadata.Table) template.TableModel {
							return template.DefaultTableModel(table).
								UseFileName(schemaMetaData.Name + "_" + table.Name).
								UseTypeName(utils.ToGoIdentifier(table.Name) + "Table")
						}).
						UseView(func(table metadata.Table) template.ViewModel {
							return template.DefaultViewModel(table).
								UseFileName(schemaMetaData.Name + "_" + table.Name + "_view").
								UseTypeName(utils.ToGoIdentifier(table.Name) + "View")
						}).
						UseEnum(func(enumMetaData metadata.Enum) template.EnumModel {
							return template.DefaultEnumModel(enumMetaData).
								UseFileName(enumMetaData.Name + "_enum").
								UseTypeName(utils.ToGoIdentifier(enumMetaData.Name) + "Enum")
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
}

func TestGeneratorTemplate_Model_SkipTableAndEnum(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		dbConnection,
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
		dbConnection,
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseSQLBuilder(template.DefaultSQLBuilder().
						UseTable(func(table metadata.Table) template.TableSQLBuilder {
							return template.TableSQLBuilder{
								Skip: true,
							}
						}).
						UseView(func(table metadata.Table) template.TableSQLBuilder {
							return template.TableSQLBuilder{
								Skip: true,
							}
						}).
						UseEnum(func(enumMetaData metadata.Enum) template.EnumSQLBuilder {
							return template.EnumSQLBuilder{
								Skip: true,
							}
						}),
					)
			}),
	)
	require.Nil(t, err)

	file2.NotExists(t, defaultTableSQLBuilderFilePath, "actor.go")
	file2.NotExists(t, defaultViewSQLBuilderFilePath, "actor_info.go")
	file2.NotExists(t, defaultEnumSQLBuilderFilePath, "mpaa_rating.go")
}

func TestGeneratorTemplate_SQLBuilder_ChangeTypeAndFileName(t *testing.T) {
	err := postgres.Generate(
		tempTestDir,
		dbConnection,
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseSQLBuilder(template.DefaultSQLBuilder().
						UseTable(func(table metadata.Table) template.TableSQLBuilder {
							return template.DefaultTableSQLBuilder(table).
								UseFileName(schemaMetaData.Name + "_" + table.Name + "_table").
								UseTypeName(utils.ToGoIdentifier(table.Name) + "TableSQLBuilder").
								UseInstanceName("T_" + utils.ToGoIdentifier(table.Name))
						}).
						UseView(func(table metadata.Table) template.ViewSQLBuilder {
							return template.DefaultViewSQLBuilder(table).
								UseFileName(schemaMetaData.Name + "_" + table.Name + "_view").
								UseTypeName(utils.ToGoIdentifier(table.Name) + "ViewSQLBuilder").
								UseInstanceName("V_" + utils.ToGoIdentifier(table.Name))
						}).
						UseEnum(func(enum metadata.Enum) template.EnumSQLBuilder {
							return template.DefaultEnumSQLBuilder(enum).
								UseFileName(schemaMetaData.Name + "_" + enum.Name + "_enum").
								UseInstanceName(utils.ToGoIdentifier(enum.Name) + "EnumSQLBuilder")
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
}

func TestGeneratorTemplate_Model_AddTags(t *testing.T) {

	err := postgres.Generate(
		tempTestDir,
		dbConnection,
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
		dbConnection,
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
		dbConnection,
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
