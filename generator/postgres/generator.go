package postgres

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/generator/internal/metadata"
	"github.com/go-jet/jet/generator/internal/metadata/postgres-metadata"
	"github.com/go-jet/jet/internal/utils"
	_ "github.com/lib/pq"
	"path"
	"path/filepath"
)

type DBConnection struct {
	Host     string
	Port     string
	User     string
	Password string
	SslMode  string
	Params   string

	DBName     string
	SchemaName string
}

func Generate(destDir string, genData DBConnection) error {

	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s %s",
		genData.Host, genData.Port, genData.User, genData.Password, genData.DBName, genData.SslMode, genData.Params)

	fmt.Println("Connecting to postgres database: " + connectionString)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Ping()

	if err != nil {
		return err
	}

	fmt.Println("Retrieving schema information...")
	schemaInfo, err := postgres_metadata.GetSchemaInfo(db, genData.DBName, genData.SchemaName)

	if err != nil {
		return err
	}

	fmt.Println("	FOUND", len(schemaInfo.TableInfos), " table(s), ", len(schemaInfo.EnumInfos), " enum(s)")

	fmt.Println("Cleaning up destination directory...")
	err = utils.CleanUpGeneratedFiles(path.Join(destDir, genData.DBName, genData.SchemaName))

	if err != nil {
		return err
	}

	fmt.Println("Generating table sql builder files...")
	err = generate(schemaInfo, destDir, "table", sqlBuilderTableTemplate, schemaInfo.TableInfos)

	if err != nil {
		return err
	}

	fmt.Println("Generating table model files...")
	err = generate(schemaInfo, destDir, "model", dataModelTemplate, schemaInfo.TableInfos)

	if err != nil {
		return err
	}

	if len(schemaInfo.EnumInfos) > 0 {
		fmt.Println("Generating enum sql builder files...")
		err = generate(schemaInfo, destDir, "enum", enumTypeTemplate, schemaInfo.EnumInfos)

		if err != nil {
			return err
		}

		fmt.Println("Generating enum model files...")
		err = generate(schemaInfo, destDir, "model", enumModelTemplate, schemaInfo.EnumInfos)

		if err != nil {
			return err
		}
	}

	fmt.Println("Done")

	return nil
}

func generate(schemaInfo postgres_metadata.SchemaInfo, dirPath, packageName string, template string, metaDataList []metadata.MetaData) error {
	modelDirPath := filepath.Join(dirPath, schemaInfo.DatabaseName, schemaInfo.Name, packageName)

	err := utils.EnsureDirPath(modelDirPath)

	if err != nil {
		return err
	}

	autoGenWarning, err := utils.GenerateTemplate(autoGenWarningTemplate, nil)

	if err != nil {
		return err
	}

	for _, metaData := range metaDataList {
		text, err := utils.GenerateTemplate(template, metaData)

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
