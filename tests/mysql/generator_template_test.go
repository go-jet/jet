package mysql

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/generator/metadata"
	mysql2 "github.com/go-jet/jet/v2/generator/mysql"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/3rdparty/snaker"
	"github.com/go-jet/jet/v2/internal/utils/dbidentifier"
	postgres2 "github.com/go-jet/jet/v2/postgres"
	file2 "github.com/go-jet/jet/v2/tests/internal/utils/file"
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

const tempTestDir = "./.tempTestDir"

var defaultModelPath = path.Join(tempTestDir, "dvds/model")
var defaultActorModelFilePath = path.Join(tempTestDir, "dvds/model", "actor.go")
var defaultTableSQLBuilderFilePath = path.Join(tempTestDir, "dvds/table")
var defaultViewSQLBuilderFilePath = path.Join(tempTestDir, "dvds/view")
var defaultEnumSQLBuilderFilePath = path.Join(tempTestDir, "dvds/enum")
var defaultActorSQLBuilderFilePath = path.Join(tempTestDir, "dvds/table", "actor.go")

func dbConnection(dbName string) mysql2.DBConnection {
	if sourceIsMariaDB() {
		return mysql2.DBConnection{
			Host:     MariaDBHost,
			Port:     MariaDBPort,
			User:     MariaDBUser,
			Password: MariaDBPassword,
			DBName:   dbName,
		}
	}

	return mysql2.DBConnection{
		Host:     MySqLHost,
		Port:     MySQLPort,
		User:     MySQLUser,
		Password: MySQLPassword,
		DBName:   dbName,
	}
}

func TestGeneratorTemplate_Schema_ChangePath(t *testing.T) {
	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).UsePath("new/schema/path")
			}),
	)

	require.Nil(t, err)

	file2.Exists(t, tempTestDir, "new/schema/path/model/actor.go")
	file2.Exists(t, tempTestDir, "new/schema/path/table/actor.go")
	file2.Exists(t, tempTestDir, "new/schema/path/view/actor_info.go")
	file2.Exists(t, tempTestDir, "new/schema/path/enum/film_rating.go")
}

func TestGeneratorTemplate_Model_SkipGeneration(t *testing.T) {
	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
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
	file2.Exists(t, defaultTableSQLBuilderFilePath, "actor.go")
	file2.Exists(t, defaultViewSQLBuilderFilePath, "actor_info.go")
	file2.Exists(t, defaultEnumSQLBuilderFilePath, "film_rating.go")
}

func TestGeneratorTemplate_SQLBuilder_SkipGeneration(t *testing.T) {
	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseSQLBuilder(template.SQLBuilder{
						Skip: true,
					})
			}),
	)

	require.Nil(t, err)

	file2.Exists(t, defaultActorModelFilePath)
	file2.NotExists(t, defaultTableSQLBuilderFilePath, "actor.go")
	file2.NotExists(t, defaultViewSQLBuilderFilePath, "actor_info.go")
	file2.NotExists(t, defaultEnumSQLBuilderFilePath, "film_rating.go")
}

func TestGeneratorTemplate_Model_ChangePath(t *testing.T) {
	const newModelPath = "/new/model/path"

	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseModel(template.DefaultModel().UsePath(newModelPath))
			}),
	)
	require.Nil(t, err)

	file2.Exists(t, tempTestDir, "dvds", newModelPath, "actor.go")
	file2.NotExists(t, defaultActorModelFilePath)
}

func TestGeneratorTemplate_SQLBuilder_ChangePath(t *testing.T) {
	const newModelPath = "/new/sql-builder/path"

	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
		template.Default(postgres2.Dialect).
			UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
				return template.DefaultSchema(schemaMetaData).
					UseSQLBuilder(template.DefaultSQLBuilder().UsePath(newModelPath))
			}),
	)
	require.Nil(t, err)

	file2.Exists(t, tempTestDir, "dvds", newModelPath, "table", "actor.go")
	file2.Exists(t, tempTestDir, "dvds", newModelPath, "view", "actor_info.go")
	file2.Exists(t, tempTestDir, "dvds", newModelPath, "enum", "film_rating.go")

	file2.NotExists(t, defaultTableSQLBuilderFilePath, "actor.go")
	file2.NotExists(t, defaultViewSQLBuilderFilePath, "actor_info.go")
	file2.NotExists(t, defaultEnumSQLBuilderFilePath, "film_rating.go")
}

func TestGeneratorTemplate_Model_RenameFilesAndTypes(t *testing.T) {
	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
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

	mpaaRating := file2.Exists(t, defaultModelPath, "film_rating_enum.go")
	require.Contains(t, mpaaRating, "type FilmRatingEnum string")
}

func TestGeneratorTemplate_Model_SkipTableAndEnum(t *testing.T) {
	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
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
	file2.NotExists(t, defaultModelPath, "film_rating.go")
}

func TestGeneratorTemplate_SQLBuilder_SkipTableAndEnum(t *testing.T) {
	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
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
	file2.NotExists(t, defaultEnumSQLBuilderFilePath, "film_rating.go")
}

func TestGeneratorTemplate_SQLBuilder_ChangeTypeAndFileName(t *testing.T) {
	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
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
	mpaaRating := file2.Exists(t, defaultEnumSQLBuilderFilePath, "dvds_film_rating_enum.go")
	require.Contains(t, mpaaRating, "var FilmRatingEnumSQLBuilder = &struct {")
}

func TestGeneratorTemplate_SQLBuilder_DefaultAlias(t *testing.T) {
	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
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

	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
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
	require.Contains(t, actor, "ActorID    uint16    `sql:\"primary_key\" json:\"actorID\" xml:\"actor_id\"`")
	require.Contains(t, actor, "FirstName  string    `json:\"firstName\" xml:\"first_name\"`")

	actorInfo := file2.Exists(t, defaultModelPath, "actor_info.go")
	require.Contains(t, actorInfo, "ActorID   uint16 `sql:\"primary_key\"`")
}

func TestGeneratorTemplate_Model_ChangeFieldTypes(t *testing.T) {
	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
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
	require.Contains(t, data, "Description        sql.NullString")
	require.Contains(t, data, "ReleaseYear        *int16")
	require.Contains(t, data, "SpecialFeatures    sql.NullString")
}

func TestGeneratorTemplate_SQLBuilder_ChangeColumnTypes(t *testing.T) {
	err := mysql2.Generate(
		tempTestDir,
		dbConnection("dvds"),
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
