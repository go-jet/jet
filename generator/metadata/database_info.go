package metadata

import (
	"database/sql"
)

type DatabaseInfo struct {
	DatabaseName string
	SchemaName   string
	TableInfos   []TableInfo
}

func GetDatabaseInfo(db *sql.DB, databaseName, schemaName string) (*DatabaseInfo, error) {

	databaseInfo := &DatabaseInfo{
		databaseName,
		schemaName,
		[]TableInfo{},
	}

	var err error
	databaseInfo.TableInfos, err = fetchTableInfos(db, databaseInfo)

	if err != nil {
		return nil, err
	}

	return databaseInfo, nil
}
