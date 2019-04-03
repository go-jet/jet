package generator

import (
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/sub0Zero/go-sqlbuilder/generator/metadata"
	"path"
)

type DbConnectInfo struct {
	host     string
	port     int
	user     string
	password string
	dbname   string
}

func Generate(folderPath string, connectString string, databaseName, schemaName string) error {

	err := cleanUpGeneratedFiles(path.Join(folderPath, databaseName, schemaName))

	if err != nil {
		return err
	}

	db, err := sql.Open("postgres", connectString)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Ping()

	if err != nil {
		return err
	}

	databaseInfo, err := metadata.GetDatabaseInfo(db, databaseName, schemaName)

	if err != nil {
		return err
	}

	err = generateSqlBuilderModel(databaseInfo, folderPath)

	if err != nil {
		return err
	}

	err = generateDataModel(databaseInfo, folderPath)

	if err != nil {
		return err
	}

	return nil
}
