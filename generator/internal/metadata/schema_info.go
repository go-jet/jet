package metadata

import (
	"database/sql"
	"fmt"
)

// SchemaInfo metadata struct
type SchemaInfo struct {
	TableInfos []MetaData
	EnumInfos  []MetaData
}

// GetSchemaInfo returns schema information from db connection.
func GetSchemaInfo(db *sql.DB, schemaName string, querySet MetaDataQuerySet) (schemaInfo SchemaInfo, err error) {

	schemaInfo.TableInfos, err = getTableInfos(db, querySet, schemaName)

	if err != nil {
		return
	}

	schemaInfo.EnumInfos, err = querySet.GetEnumsMetaData(db, schemaName)

	if err != nil {
		return
	}

	fmt.Println("	FOUND", len(schemaInfo.TableInfos), "table(s), ", len(schemaInfo.EnumInfos), "enum(s)")

	return
}

func getTableInfos(db *sql.DB, querySet MetaDataQuerySet, schemaName string) ([]MetaData, error) {

	fmt.Println(querySet.ListOfTablesQuery())

	rows, err := db.Query(querySet.ListOfTablesQuery(), schemaName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ret := []MetaData{}
	for rows.Next() {
		var tableName string

		err = rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}

		fmt.Println(tableName)

		tableInfo, err := GetTableInfo(db, querySet, schemaName, tableName)

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
