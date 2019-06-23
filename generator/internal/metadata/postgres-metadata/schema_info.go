package postgres_metadata

import (
	"database/sql"
	"github.com/go-jet/jet/generator/internal/metadata"
)

type SchemaInfo struct {
	DatabaseName string
	Name         string
	TableInfos   []metadata.MetaData
	EnumInfos    []metadata.MetaData
}

func GetSchemaInfo(db *sql.DB, databaseName, schemaName string) (schemaInfo SchemaInfo, err error) {

	schemaInfo.DatabaseName = databaseName
	schemaInfo.Name = schemaName

	schemaInfo.TableInfos, err = getTableInfos(db, databaseName, schemaName)

	if err != nil {
		return
	}

	schemaInfo.EnumInfos, err = getEnumInfos(db, schemaName)

	if err != nil {
		return
	}

	return
}

func getTableInfos(db *sql.DB, dbName, schemaName string) ([]metadata.MetaData, error) {

	query := `
SELECT table_name 
FROM information_schema.tables
where table_catalog = $1 and table_schema = $2 and table_type = 'BASE TABLE';
`

	rows, err := db.Query(query, dbName, schemaName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ret := []metadata.MetaData{}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}

		tableInfo, err := GetTableInfo(db, dbName, schemaName, tableName)

		if err != nil {
			return nil, err
		}

		ret = append(ret, tableInfo)
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return ret, nil
}
