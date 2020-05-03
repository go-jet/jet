package template

import (
	"bytes"
	"fmt"
	"github.com/go-jet/jet/generator/internal/metadata"
	"github.com/go-jet/jet/internal/jet"
	"github.com/go-jet/jet/internal/utils"
	"path/filepath"
	"text/template"
	"time"
)

// GenerateFiles generates Go files from tables and enums metadata
func GenerateFiles(destDir string, schemaInfo metadata.SchemaMetaData, dialect jet.Dialect) {
	if schemaInfo.IsEmpty() {
		return
	}

	fmt.Println("Destination directory:", destDir)
	fmt.Println("Cleaning up destination directory...")
	err := utils.CleanUpGeneratedFiles(destDir)
	utils.PanicOnError(err)

	tableSQLBuilderTemplate := getTableSQLBuilderTemplate(dialect)
	generateSQLBuilderFiles(destDir, "table", tableSQLBuilderTemplate, schemaInfo.TablesMetaData, dialect)
	generateSQLBuilderFiles(destDir, "view", tableSQLBuilderTemplate, schemaInfo.ViewsMetaData, dialect)
	generateSQLBuilderFiles(destDir, "enum", enumSQLBuilderTemplate, schemaInfo.EnumsMetaData, dialect)

	generateModelFiles(destDir, "table", tableModelTemplate, schemaInfo.TablesMetaData, dialect)
	generateModelFiles(destDir, "view", tableModelTemplate, schemaInfo.ViewsMetaData, dialect)
	generateModelFiles(destDir, "enum", enumModelTemplate, schemaInfo.EnumsMetaData, dialect)

	fmt.Println("Done")
}

func getTableSQLBuilderTemplate(dialect jet.Dialect) string {
	if dialect.Name() == "PostgreSQL" {
		return tablePostgreSQLBuilderTemplate
	}

	return tableSQLBuilderTemplate
}

func generateSQLBuilderFiles(destDir, fileTypes, sqlBuilderTemplate string, metaData []metadata.MetaData, dialect jet.Dialect) {
	if len(metaData) == 0 {
		return
	}
	fmt.Printf("Generating %s sql builder files...\n", fileTypes)
	generateGoFiles(destDir, fileTypes, sqlBuilderTemplate, metaData, dialect)
}

func generateModelFiles(destDir, fileTypes, modelTemplate string, metaData []metadata.MetaData, dialect jet.Dialect) {
	if len(metaData) == 0 {
		return
	}
	fmt.Printf("Generating %s model files...\n", fileTypes)
	generateGoFiles(destDir, "model", modelTemplate, metaData, dialect)
}

func generateGoFiles(dirPath, packageName string, template string, metaDataList []metadata.MetaData, dialect jet.Dialect) {
	modelDirPath := filepath.Join(dirPath, packageName)

	err := utils.EnsureDirPath(modelDirPath)
	utils.PanicOnError(err)

	autoGenWarning, err := GenerateTemplate(autoGenWarningTemplate, nil, dialect)
	utils.PanicOnError(err)

	for _, metaData := range metaDataList {
		text, err := GenerateTemplate(template, metaData, dialect, map[string]interface{}{"package": packageName})
		utils.PanicOnError(err)

		err = utils.SaveGoFile(modelDirPath, utils.ToGoFileName(metaData.Name()), append(autoGenWarning, text...))
		utils.PanicOnError(err)
	}

	return
}

// GenerateTemplate generates template with template text and template data.
func GenerateTemplate(templateText string, templateData interface{}, dialect jet.Dialect, params ...map[string]interface{}) ([]byte, error) {

	t, err := template.New("sqlBuilderTableTemplate").Funcs(template.FuncMap{
		"ToGoIdentifier":          utils.ToGoIdentifier,
		"ToGoEnumValueIdentifier": utils.ToGoEnumValueIdentifier,
		"now": func() string {
			return time.Now().Format(time.RFC850)
		},
		"dialect": func() jet.Dialect {
			return dialect
		},
		"param": func(name string) interface{} {
			if len(params) > 0 {
				return params[0][name]
			}
			return ""
		},
	}).Parse(templateText)

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, templateData); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
