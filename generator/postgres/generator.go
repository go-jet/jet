package postgres

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/generator/internal/metadata"
	"github.com/go-jet/jet/generator/internal/metadata/postgresmeta"
	"github.com/go-jet/jet/internal/utils"
	"path"
	"path/filepath"
)

// DBConnection contains postgres connection details
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

// Generate generates jet files at destination dir from database connection details
func Generate(destDir string, dbConn DBConnection) error {

	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s %s",
		dbConn.Host, dbConn.Port, dbConn.User, dbConn.Password, dbConn.DBName, dbConn.SslMode, dbConn.Params)

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
	schemaInfo, err := postgresmeta.GetSchemaInfo(db, dbConn.DBName, dbConn.SchemaName)

	if err != nil {
		return err
	}

	fmt.Println("	FOUND", len(schemaInfo.TableInfos), "table(s), ", len(schemaInfo.EnumInfos), "enum(s)")

	if len(schemaInfo.TableInfos) == 0 && len(schemaInfo.EnumInfos) == 0 {
		return nil
	}

	schemaGenPath := path.Join(destDir, dbConn.DBName, dbConn.SchemaName)
	fmt.Println("Destination directory:", schemaGenPath)
	fmt.Println("Cleaning up destination directory...")
	err = utils.CleanUpGeneratedFiles(schemaGenPath)

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

func generate(schemaInfo postgresmeta.SchemaInfo, dirPath, packageName string, template string, metaDataList []metadata.MetaData) error {
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
