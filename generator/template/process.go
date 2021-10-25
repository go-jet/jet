package template

import (
	"bytes"
	"fmt"
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"path"
	"strings"
	"text/template"
)

// ProcessSchema will process schema metadata and constructs go files using generator Template
func ProcessSchema(dirPath string, schemaMetaData metadata.Schema, generatorTemplate Template) {
	if schemaMetaData.IsEmpty() {
		return
	}

	schemaTemplate := generatorTemplate.Schema(schemaMetaData)
	schemaPath := path.Join(dirPath, schemaTemplate.Path)

	fmt.Println("Destination directory:", schemaPath)
	fmt.Println("Cleaning up destination directory...")
	err := utils.CleanUpGeneratedFiles(schemaPath)
	throw.OnError(err)

	processModel(schemaPath, schemaMetaData, schemaTemplate)
	processSQLBuilder(schemaPath, generatorTemplate.Dialect, schemaMetaData, schemaTemplate)
}

func processModel(dirPath string, schemaMetaData metadata.Schema, schemaTemplate Schema) {
	modelTemplate := schemaTemplate.Model

	if modelTemplate.Skip {
		fmt.Println("Skipping the generation of model types.")
		return
	}

	modelDirPath := path.Join(dirPath, modelTemplate.Path)

	err := utils.EnsureDirPath(modelDirPath)
	throw.OnError(err)

	processTableModels("table", modelDirPath, schemaMetaData.TablesMetaData, modelTemplate)
	processTableModels("view", modelDirPath, schemaMetaData.ViewsMetaData, modelTemplate)
	processEnumModels(modelDirPath, schemaMetaData.EnumsMetaData, modelTemplate)
}

func processSQLBuilder(dirPath string, dialect jet.Dialect, schemaMetaData metadata.Schema, schemaTemplate Schema) {
	sqlBuilderTemplate := schemaTemplate.SQLBuilder

	if sqlBuilderTemplate.Skip {
		fmt.Println("Skipping the generation of SQL Builder types.")
		return
	}

	sqlBuilderPath := path.Join(dirPath, sqlBuilderTemplate.Path)

	processTableSQLBuilder("table", sqlBuilderPath, dialect, schemaMetaData, schemaMetaData.TablesMetaData, sqlBuilderTemplate)
	processTableSQLBuilder("view", sqlBuilderPath, dialect, schemaMetaData, schemaMetaData.ViewsMetaData, sqlBuilderTemplate)
	processEnumSQLBuilder(sqlBuilderPath, dialect, schemaMetaData.EnumsMetaData, sqlBuilderTemplate)
}

func processEnumSQLBuilder(dirPath string, dialect jet.Dialect, enumsMetaData []metadata.Enum, sqlBuilder SQLBuilder) {
	if len(enumsMetaData) == 0 {
		return
	}

	fmt.Printf("Generating enum sql builder files\n")

	for _, enumMetaData := range enumsMetaData {
		enumTemplate := sqlBuilder.Enum(enumMetaData)

		if enumTemplate.Skip {
			continue
		}

		enumSQLBuilderPath := path.Join(dirPath, enumTemplate.Path)

		err := utils.EnsureDirPath(enumSQLBuilderPath)
		throw.OnError(err)

		text, err := generateTemplate(
			autoGenWarningTemplate+enumSQLBuilderTemplate,
			enumMetaData,
			template.FuncMap{
				"package": func() string {
					return enumTemplate.PackageName()
				},
				"dialect": func() jet.Dialect {
					return dialect
				},
				"enumTemplate": func() EnumSQLBuilder {
					return enumTemplate
				},
				"enumValueName": func(enumValue string) string {
					return enumTemplate.ValueName(enumValue)
				},
			})
		throw.OnError(err)

		err = utils.SaveGoFile(enumSQLBuilderPath, enumTemplate.FileName, text)
		throw.OnError(err)
	}
}

func processTableSQLBuilder(fileTypes, dirPath string,
	dialect jet.Dialect,
	schemaMetaData metadata.Schema,
	tablesMetaData []metadata.Table,
	sqlBuilderTemplate SQLBuilder) {

	if len(tablesMetaData) == 0 {
		return
	}

	fmt.Printf("Generating %s sql builder files\n", fileTypes)

	for _, tableMetaData := range tablesMetaData {

		var tableSQLBuilderTemplate TableSQLBuilder

		if fileTypes == "view" {
			tableSQLBuilderTemplate = sqlBuilderTemplate.View(tableMetaData)
		} else {
			tableSQLBuilderTemplate = sqlBuilderTemplate.Table(tableMetaData)
		}

		if tableSQLBuilderTemplate.Skip {
			continue
		}

		tableSQLBuilderPath := path.Join(dirPath, tableSQLBuilderTemplate.Path)

		err := utils.EnsureDirPath(tableSQLBuilderPath)
		throw.OnError(err)

		text, err := generateTemplate(
			autoGenWarningTemplate+getTableSQLBuilderTemplate(dialect),
			tableMetaData,
			template.FuncMap{
				"package": func() string {
					return tableSQLBuilderTemplate.PackageName()
				},
				"dialect": func() jet.Dialect {
					return dialect
				},
				"schemaName": func() string {
					return schemaMetaData.Name
				},
				"tableTemplate": func() TableSQLBuilder {
					return tableSQLBuilderTemplate
				},
				"structImplName": func() string { // postgres only
					structName := tableSQLBuilderTemplate.TypeName
					return string(strings.ToLower(structName)[0]) + structName[1:]
				},
				"columnField": func(columnMetaData metadata.Column) TableSQLBuilderColumn {
					return tableSQLBuilderTemplate.Column(columnMetaData)
				},
			})
		throw.OnError(err)

		err = utils.SaveGoFile(tableSQLBuilderPath, tableSQLBuilderTemplate.FileName, text)
		throw.OnError(err)
	}
}

func getTableSQLBuilderTemplate(dialect jet.Dialect) string {
	if dialect.Name() == "PostgreSQL" || dialect.Name() == "SQLite" {
		return tableSQLBuilderTemplateWithEXCLUDED
	}

	return tableSQLBuilderTemplate
}

func processTableModels(fileTypes, modelDirPath string, tablesMetaData []metadata.Table, modelTemplate Model) {
	if len(tablesMetaData) == 0 {
		return
	}
	fmt.Printf("Generating %s model files...\n", fileTypes)

	for _, tableMetaData := range tablesMetaData {
		var tableTemplate TableModel

		if fileTypes == "table" {
			tableTemplate = modelTemplate.Table(tableMetaData)
		} else {
			tableTemplate = modelTemplate.View(tableMetaData)
		}

		if tableTemplate.Skip {
			continue
		}

		text, err := generateTemplate(
			autoGenWarningTemplate+tableModelFileTemplate,
			tableMetaData,
			template.FuncMap{
				"package": func() string {
					return modelTemplate.PackageName()
				},
				"modelImports": func() []string {
					return getTableModelImports(tableTemplate, tableMetaData)
				},
				"tableTemplate": func() TableModel {
					return tableTemplate
				},
				"structField": func(columnMetaData metadata.Column) TableModelField {
					return tableTemplate.Field(columnMetaData)
				},
			})
		throw.OnError(err)

		err = utils.SaveGoFile(modelDirPath, tableTemplate.FileName, text)
		throw.OnError(err)
	}
}

func processEnumModels(modelDir string, enumsMetaData []metadata.Enum, modelTemplate Model) {
	if len(enumsMetaData) == 0 {
		return
	}
	fmt.Print("Generating enum model files...\n")

	for _, enumMetaData := range enumsMetaData {
		enumTemplate := modelTemplate.Enum(enumMetaData)

		if enumTemplate.Skip {
			continue
		}

		text, err := generateTemplate(
			autoGenWarningTemplate+enumModelTemplate,
			enumMetaData,
			template.FuncMap{
				"package": func() string {
					return modelTemplate.PackageName()
				},
				"enumTemplate": func() EnumModel {
					return enumTemplate
				},
				"valueName": func(value string) string {
					return enumTemplate.ValueName(value)
				},
			})
		throw.OnError(err)

		err = utils.SaveGoFile(modelDir, enumTemplate.FileName, text)
		throw.OnError(err)
	}
}

func generateTemplate(templateText string, templateData interface{}, funcMap template.FuncMap) ([]byte, error) {
	t, err := template.New("sqlBuilderTableTemplate").Funcs(funcMap).Parse(templateText)

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, templateData); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
