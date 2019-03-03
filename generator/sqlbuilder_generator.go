package generator

import (
	"bytes"
	"github.com/serenize/snaker"
	"go/format"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

func generateSqlBuilderModel(databaseName, schemaName string, tableInfo TableInfo, dirPath string) error {

	schemaDirPath := filepath.Join(dirPath, databaseName, schemaName, "table")

	if _, err := os.Stat(schemaDirPath); os.IsNotExist(err) {
		err := os.MkdirAll(schemaDirPath, os.ModePerm)

		if err != nil {
			return err
		}
	}

	t, err := template.New("TableTemplate").Funcs(template.FuncMap{
		"camelize": func(txt string) string {
			return snaker.SnakeToCamel(txt)
		},
		"columnName": columnName,
	}).Parse(TableTemplate)

	if err != nil {
		return err
	}

	newGoFilePath := filepath.Join(schemaDirPath, tableInfo.Name) + ".go"

	file, err := os.Create(newGoFilePath)

	if err != nil {
		return err
	}

	defer file.Close()

	tableTemplate := TableTemplateData{
		databaseName,
		tableInfo,
	}

	//err = t.Execute(file, &tableTemplate)
	//
	//if err != nil {
	//	return err
	//}

	var buf bytes.Buffer
	if err := t.Execute(&buf, &tableTemplate); err != nil {
		return err
	}
	p, err := format.Source(buf.Bytes())
	if err != nil {
		return err
	}

	_, err = file.Write(p)

	if err != nil {
		return err
	}

	return nil
}

type TableTemplateData struct {
	PackageName string
	TableInfo   TableInfo
}

func columnName(table, column string) string {
	return snaker.SnakeToCamelLower(table) + snaker.SnakeToCamel(column) + "Column"
}

func (t *TableTemplateData) ColumnNameList(sep string) string {
	columnNames := []string{}
	for _, columnInfo := range t.TableInfo.Columns {
		columnInfoName := columnInfo.Name
		columnNames = append(columnNames, columnName(t.TableInfo.Name, columnInfoName))
	}
	return strings.Join(columnNames, sep)
}
