package generator

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/generator/metadata"
	"github.com/go-jet/jet/generator/postgres-metadata"
	_ "github.com/lib/pq"
	"path"
	"path/filepath"
)

type GeneratorData struct {
	Host     string
	Port     string
	User     string
	Password string
	SslMode  string
	Params   string

	DbName     string
	SchemaName string
}

func Generate(destDir string, genData GeneratorData) error {

	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s %s",
		genData.Host, genData.Port, genData.User, genData.Password, genData.DbName, genData.SslMode, genData.Params)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Ping()

	if err != nil {
		return err
	}

	err = cleanUpGeneratedFiles(path.Join(destDir, genData.DbName, genData.SchemaName))

	if err != nil {
		return err
	}

	schemaInfo, err := postgres_metadata.GetSchemaInfo(db, genData.DbName, genData.SchemaName)

	if err != nil {
		return err
	}

	err = generate(schemaInfo, destDir, "table", sqlBuilderTableTemplate, schemaInfo.TableInfos)

	if err != nil {
		return err
	}

	//err = generateDataModel(schemaInfo, destDir)
	err = generate(schemaInfo, destDir, "model", dataModelTemplate, schemaInfo.TableInfos)

	if err != nil {
		return err
	}

	err = generate(schemaInfo, destDir, "model", enumModelTemplate, schemaInfo.EnumInfos)

	if err != nil {
		return err
	}

	err = generate(schemaInfo, destDir, "enum", enumTypeTemplate, schemaInfo.EnumInfos)

	if err != nil {
		return err
	}

	return nil
}

func generate(schemaInfo postgres_metadata.SchemaInfo, dirPath, packageName string, template string, metaDataList []metadata.MetaData) error {
	modelDirPath := filepath.Join(dirPath, schemaInfo.DatabaseName, schemaInfo.Name, packageName)

	err := ensureDirPath(modelDirPath)

	if err != nil {
		return err
	}

	autoGenWarning, err := generateTemplate(autoGenWarningTemplate, nil)

	if err != nil {
		return err
	}

	for _, metaData := range metaDataList {
		text, err := generateTemplate(template, metaData)

		if err != nil {
			return err
		}

		err = saveGoFile(modelDirPath, metaData.Name(), append(autoGenWarning, text...))

		if err != nil {
			return err
		}
	}

	return nil
}
