package template

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils/filesys"
	"path"
	"strings"
	"text/template"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/internal/jet"
)

// ProcessSchema will process schema metadata and constructs go files using generator Template
func ProcessSchema(dirPath string, schemaMetaData metadata.Schema, generatorTemplate Template) error {
	if schemaMetaData.IsEmpty() {
		return nil
	}

	schemaTemplate := generatorTemplate.Schema(schemaMetaData)
	schemaPath := path.Join(dirPath, schemaTemplate.Path)

	fmt.Println("Destination directory:", schemaPath)
	fmt.Println("Cleaning up destination directory...")
	err := filesys.RemoveDir(schemaPath)
	if err != nil {
		return errors.New("failed to cleanup generated files")
	}

	err = processModel(schemaPath, schemaMetaData, schemaTemplate)
	if err != nil {
		return fmt.Errorf("failed to generate model types: %w", err)
	}

	err = processSQLBuilder(schemaPath, generatorTemplate.Dialect, schemaMetaData, schemaTemplate)
	if err != nil {
		return fmt.Errorf("failed to generate sql builder types: %w", err)
	}

	return nil
}

func processModel(dirPath string, schemaMetaData metadata.Schema, schemaTemplate Schema) error {
	modelTemplate := schemaTemplate.Model

	if modelTemplate.Skip {
		fmt.Println("Skipping the generation of model types.")
		return nil
	}

	modelDirPath := path.Join(dirPath, modelTemplate.Path)

	err := filesys.EnsureDirPathExist(modelDirPath)
	if err != nil {
		return fmt.Errorf("destination dir path does not exist: %w", err)
	}

	err = processTableModels("table", modelDirPath, schemaMetaData.TablesMetaData, modelTemplate)
	if err != nil {
		return fmt.Errorf("failed to generate table model types: %w", err)
	}

	err = processTableModels("view", modelDirPath, schemaMetaData.ViewsMetaData, modelTemplate)
	if err != nil {
		return fmt.Errorf("failed to generate view model types: %w", err)
	}

	err = processEnumModels(modelDirPath, schemaMetaData.EnumsMetaData, modelTemplate)
	if err != nil {
		return fmt.Errorf("failed to process enum types: %w", err)
	}

	return nil
}

func processSQLBuilder(dirPath string, dialect jet.Dialect, schemaMetaData metadata.Schema, schemaTemplate Schema) error {
	sqlBuilderTemplate := schemaTemplate.SQLBuilder

	if sqlBuilderTemplate.Skip {
		fmt.Println("Skipping the generation of SQL Builder types.")
		return nil
	}

	sqlBuilderPath := path.Join(dirPath, sqlBuilderTemplate.Path)

	err := processTableSQLBuilder("table", sqlBuilderPath, dialect, schemaMetaData, schemaMetaData.TablesMetaData, sqlBuilderTemplate)
	if err != nil {
		return fmt.Errorf("failed to process table sql builder types: %w", err)
	}

	err = processTableSQLBuilder("view", sqlBuilderPath, dialect, schemaMetaData, schemaMetaData.ViewsMetaData, sqlBuilderTemplate)
	if err != nil {
		return fmt.Errorf("failed to process view sql builder types: %w", err)
	}

	err = processEnumSQLBuilder(sqlBuilderPath, dialect, schemaMetaData.EnumsMetaData, sqlBuilderTemplate)
	if err != nil {
		return fmt.Errorf("failed to process enum types: %w", err)
	}

	return nil
}

func processEnumSQLBuilder(dirPath string, dialect jet.Dialect, enumsMetaData []metadata.Enum, sqlBuilder SQLBuilder) error {
	if len(enumsMetaData) == 0 {
		return nil
	}

	fmt.Printf("Generating enum sql builder files\n")

	for _, enumMetaData := range enumsMetaData {
		enumTemplate := sqlBuilder.Enum(enumMetaData)

		if enumTemplate.Skip {
			continue
		}

		enumSQLBuilderPath := path.Join(dirPath, enumTemplate.Path)

		err := filesys.EnsureDirPathExist(enumSQLBuilderPath)
		if err != nil {
			return fmt.Errorf("failed to create enum sql builder directory - %s: %w", enumSQLBuilderPath, err)
		}

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
				"golangComment": formatGolangComment,
			})
		if err != nil {
			return fmt.Errorf("failed to generete enum type %s: %w", enumTemplate.FileName, err)
		}

		err = filesys.FormatAndSaveGoFile(enumSQLBuilderPath, enumTemplate.FileName, text)
		if err != nil {
			return fmt.Errorf("failed to format and save '%s' enum type : %w", enumTemplate.FileName, err)
		}
	}

	return nil
}

func processTableSQLBuilder(fileTypes, dirPath string,
	dialect jet.Dialect,
	schemaMetaData metadata.Schema,
	tablesMetaData []metadata.Table,
	sqlBuilderTemplate SQLBuilder) error {

	if len(tablesMetaData) == 0 {
		return nil
	}

	fmt.Printf("Generating %s sql builder files\n", fileTypes)

	var generatedBuilders []TableSQLBuilder

	for _, tableMetaData := range tablesMetaData {
		var tableSQLBuilder TableSQLBuilder

		if fileTypes == "view" {
			tableSQLBuilder = sqlBuilderTemplate.View(tableMetaData)
		} else {
			tableSQLBuilder = sqlBuilderTemplate.Table(tableMetaData)
		}

		if tableSQLBuilder.Skip {
			continue
		}

		tableSQLBuilderPath := path.Join(dirPath, tableSQLBuilder.Path)

		err := filesys.EnsureDirPathExist(tableSQLBuilderPath)
		if err != nil {
			return fmt.Errorf("failed to create table sql builder directory - %s: %w", tableSQLBuilderPath, err)
		}

		text, err := generateTemplate(
			autoGenWarningTemplate+tableSQLBuilderTemplate,
			tableMetaData,
			template.FuncMap{
				"package": func() string {
					return tableSQLBuilder.PackageName()
				},
				"dialect": func() jet.Dialect {
					return dialect
				},
				"schemaName": func() string {
					return schemaMetaData.Name
				},
				"tableTemplate": func() TableSQLBuilder {
					return tableSQLBuilder
				},
				"structImplName": func() string { // postgres only
					structName := tableSQLBuilder.TypeName
					return string(strings.ToLower(structName)[0]) + structName[1:]
				},
				"columnField": func(columnMetaData metadata.Column) TableSQLBuilderColumn {
					return tableSQLBuilder.Column(columnMetaData)
				},
				"toUpper": strings.ToUpper,
				"insertedRowAlias": func() string {
					return insertedRowAlias(dialect)
				},
				"golangComment": formatGolangComment,
			})
		if err != nil {
			return fmt.Errorf("failed to generate table sql builder type %s: %w", tableSQLBuilder.TypeName, err)
		}

		err = filesys.FormatAndSaveGoFile(tableSQLBuilderPath, tableSQLBuilder.FileName, text)
		if err != nil {
			return fmt.Errorf("failed to format and save generated sql builder type '%s': %w", tableSQLBuilder.FileName, err)
		}

		generatedBuilders = append(generatedBuilders, tableSQLBuilder)
	}

	err := generateUseSchemaFunc(dirPath, fileTypes, generatedBuilders)
	if err != nil {
		return fmt.Errorf("failed to generate UseSchema function")
	}

	return nil
}

func generateUseSchemaFunc(dirPath, fileTypes string, builders []TableSQLBuilder) error {
	if len(builders) == 0 {
		return nil
	}

	text, err := generateTemplate(
		autoGenWarningTemplate+tableSqlBuilderSetSchemaTemplate,
		builders,
		template.FuncMap{
			"package": func() string { return builders[0].PackageName() },
			"type":    func() string { return fileTypes },
		},
	)
	if err != nil {
		return fmt.Errorf("failed to generate use schema template: %w", err)
	}

	basePath := path.Join(dirPath, builders[0].Path)
	fileName := fileTypes + "_use_schema"

	err = filesys.FormatAndSaveGoFile(basePath, fileName, text)
	if err != nil {
		return fmt.Errorf("failed to save %s file: %w", fileName, err)
	}

	return nil
}

func insertedRowAlias(dialect jet.Dialect) string {
	if dialect.Name() == "MySQL" {
		return "new"
	}

	return "excluded"
}

func processTableModels(fileTypes, modelDirPath string, tablesMetaData []metadata.Table, modelTemplate Model) error {
	if len(tablesMetaData) == 0 {
		return nil
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
				"golangComment": formatGolangComment,
			})
		if err != nil {
			return fmt.Errorf("failed to generate model type '%s': %w", tableMetaData.Name, err)
		}

		err = filesys.FormatAndSaveGoFile(modelDirPath, tableTemplate.FileName, text)
		if err != nil {
			return fmt.Errorf("failed to save '%s' model type: %w", tableTemplate.FileName, err)
		}
	}

	return nil
}

func processEnumModels(modelDir string, enumsMetaData []metadata.Enum, modelTemplate Model) error {
	if len(enumsMetaData) == 0 {
		return nil
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
				"golangComment": formatGolangComment,
			})

		if err != nil {
			return fmt.Errorf("failed to generate enum type '%s': %w", enumMetaData.Name, err)
		}

		err = filesys.FormatAndSaveGoFile(modelDir, enumTemplate.FileName, text)
		if err != nil {
			return fmt.Errorf("failed to save '%s' enum type: %w", enumTemplate.FileName, err)
		}
	}

	return nil
}

func generateTemplate(templateText string, templateData interface{}, funcMap template.FuncMap) ([]byte, error) {
	t, err := template.New("sqlBuilderTableTemplate").Funcs(funcMap).Parse(templateText)

	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, templateData); err != nil {
		return nil, fmt.Errorf("failed to generate template: %w", err)
	}

	return buf.Bytes(), nil
}
