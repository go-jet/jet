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
func GenerateFiles(destDir string, schemaInfo metadata.SchemaMetaData, dialect jet.Dialect) error {
	if schemaInfo.IsEmpty() {
		return nil
	}

	fmt.Println("Destination directory:", destDir)
	fmt.Println("Cleaning up destination directory...")
	err := utils.CleanUpGeneratedFiles(destDir)

	if err != nil {
		return err
	}

	err = generateSQLBuilderFiles(destDir, "table", tableSQLBuilderTemplate, schemaInfo.TablesMetaData, dialect)

	if err != nil {
		return err
	}

	err = generateSQLBuilderFiles(destDir, "view", tableSQLBuilderTemplate, schemaInfo.ViewsMetaData, dialect)

	if err != nil {
		return err
	}

	err = generateSQLBuilderFiles(destDir, "enum", enumSQLBuilderTemplate, schemaInfo.EnumsMetaData, dialect)

	if err != nil {
		return err
	}

	err = generateModelFiles(destDir, "table", tableModelTemplate, schemaInfo.TablesMetaData, dialect)

	if err != nil {
		return err
	}

	err = generateModelFiles(destDir, "view", tableModelTemplate, schemaInfo.ViewsMetaData, dialect)

	if err != nil {
		return err
	}

	err = generateModelFiles(destDir, "enum", enumModelTemplate, schemaInfo.EnumsMetaData, dialect)

	if err != nil {
		return err
	}

	fmt.Println("Done")

	return nil
}

func generateSQLBuilderFiles(destDir, fileTypes, sqlBuilderTemplate string, metaData []metadata.MetaData, dialect jet.Dialect) error {
	if len(metaData) == 0 {
		return nil
	}
	fmt.Printf("Generating %s sql builder files...\n", fileTypes)
	return generateGoFiles(destDir, fileTypes, sqlBuilderTemplate, metaData, dialect)
}

func generateModelFiles(destDir, fileTypes, modelTemplate string, metaData []metadata.MetaData, dialect jet.Dialect) error {
	if len(metaData) == 0 {
		return nil
	}
	fmt.Printf("Generating %s model files...\n", fileTypes)
	return generateGoFiles(destDir, "model", modelTemplate, metaData, dialect)
}

func generateGoFiles(dirPath, packageName string, template string, metaDataList []metadata.MetaData, dialect jet.Dialect) error {
	modelDirPath := filepath.Join(dirPath, packageName)

	err := utils.EnsureDirPath(modelDirPath)

	if err != nil {
		return err
	}

	autoGenWarning, err := GenerateTemplate(autoGenWarningTemplate, nil, dialect)

	if err != nil {
		return err
	}

	for _, metaData := range metaDataList {
		text, err := GenerateTemplate(template, metaData, dialect, map[string]interface{}{"package": packageName})

		if err != nil {
			return err
		}

		err = utils.SaveGoFile(modelDirPath, utils.ToGoFileName(metaData.Name()), append(autoGenWarning, text...))

		if err != nil {
			return err
		}
	}

	return nil
}

// GenerateTemplate generates template with template text and template data.
func GenerateTemplate(templateText string, templateData interface{}, dialect jet.Dialect, params ...map[string]interface{}) ([]byte, error) {

	t, err := template.New("sqlBuilderTableTemplate").Funcs(template.FuncMap{
		"ToGoIdentifier": utils.ToGoIdentifier,
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
