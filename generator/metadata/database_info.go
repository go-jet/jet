package metadata

import (
	"database/sql"
)

type DatabaseInfo struct {
	DatabaseName string
	SchemaName   string
	TableInfos   []TableInfo
	EnumInfos    []EnumInfo
}

func GetDatabaseInfo(db *sql.DB, databaseName, schemaName string) (*DatabaseInfo, error) {

	databaseInfo := &DatabaseInfo{
		databaseName,
		schemaName,
		[]TableInfo{},
		[]EnumInfo{},
	}

	var err error
	databaseInfo.TableInfos, err = fetchTableInfos(db, databaseInfo)

	if err != nil {
		return nil, err
	}

	databaseInfo.EnumInfos, err = fetchEnumInfos(db, databaseInfo)

	if err != nil {
		return nil, err
	}

	return databaseInfo, nil
}
