package template

import (
	"bytes"
	"fmt"
	"github.com/go-jet/jet"
	"github.com/go-jet/jet/generator/internal/metadata"
	"github.com/go-jet/jet/internal/utils"
	"path/filepath"
	"text/template"
	"time"
)

func GenerateFiles(destDir string, tables, enums []metadata.MetaData, dialect jet.Dialect) error {
	if len(tables) == 0 && len(enums) == 0 {
		return nil
	}

	fmt.Println("Destination directory:", destDir)
	fmt.Println("Cleaning up destination directory...")
	err := utils.CleanUpGeneratedFiles(destDir)

	if err != nil {
		return err
	}

	fmt.Println("Generating table sql builder files...")
	err = generate(destDir, "table", tableSQLBuilderTemplate, tables, dialect)

	if err != nil {
		return err
	}

	fmt.Println("Generating table model files...")
	err = generate(destDir, "model", tableModelTemplate, tables, dialect)

	if err != nil {
		return err
	}

	if len(enums) > 0 {
		fmt.Println("Generating enum sql builder files...")
		err = generate(destDir, "enum", enumSQLBuilderTemplate, enums, dialect)

		if err != nil {
			return err
		}

		fmt.Println("Generating enum model files...")
		err = generate(destDir, "model", enumModelTemplate, enums, dialect)

		if err != nil {
			return err
		}
	}

	fmt.Println("Done")

	return nil

}

func generate(dirPath, packageName string, template string, metaDataList []metadata.MetaData, dialect jet.Dialect) error {
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
		text, err := GenerateTemplate(template, metaData, dialect)

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
func GenerateTemplate(templateText string, templateData interface{}, dialect jet.Dialect) ([]byte, error) {

	t, err := template.New("sqlBuilderTableTemplate").Funcs(template.FuncMap{
		"ToGoIdentifier": utils.ToGoIdentifier,
		"now": func() string {
			return time.Now().Format(time.RFC850)
		},
		"dialect": func() jet.Dialect {
			return dialect
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
