package generator

import (
	"github.com/sub0zero/go-sqlbuilder/generator/metadata"
	"path/filepath"
)

func generateSqlBuilderModel(databaseInfo *metadata.DatabaseInfo, dirPath string) error {
	modelDirPath := filepath.Join(dirPath, databaseInfo.DatabaseName, databaseInfo.SchemaName, "table")

	err := ensureDirPath(modelDirPath)

	if err != nil {
		return err
	}

	for _, tableInfo := range databaseInfo.TableInfos {
		text, err := generateTemplate(SqlBuilderTableTemplate, tableInfo)

		if err != nil {
			return err
		}

		err = saveGoFile(modelDirPath, tableInfo.Name+"_table", text)

		if err != nil {
			return err
		}
	}

	return nil
}
