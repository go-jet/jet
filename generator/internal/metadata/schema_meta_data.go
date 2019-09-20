package metadata

import (
	"database/sql"
	"fmt"
)

// SchemaMetaData struct
type SchemaMetaData struct {
	TablesMetaData []MetaData
	ViewsMetaData  []MetaData
	EnumsMetaData  []MetaData
}

// IsEmpty returns true if schema info does not contain any table, views or enums metadata
func (s SchemaMetaData) IsEmpty() bool {
	return len(s.TablesMetaData) == 0 && len(s.ViewsMetaData) == 0 && len(s.EnumsMetaData) == 0
}

const (
	baseTable = "BASE TABLE"
	view      = "VIEW"
)

// GetSchemaMetaData returns schema information from db connection.
func GetSchemaMetaData(db *sql.DB, schemaName string, querySet DialectQuerySet) (schemaInfo SchemaMetaData, err error) {

	schemaInfo.TablesMetaData, err = getTablesMetaData(db, querySet, schemaName, baseTable)

	if err != nil {
		return
	}

	schemaInfo.ViewsMetaData, err = getTablesMetaData(db, querySet, schemaName, view)

	if err != nil {
		return
	}

	schemaInfo.EnumsMetaData, err = querySet.GetEnumsMetaData(db, schemaName)

	if err != nil {
		return
	}

	fmt.Println("	FOUND", len(schemaInfo.TablesMetaData), "table(s),", len(schemaInfo.ViewsMetaData), "view(s),",
		len(schemaInfo.EnumsMetaData), "enum(s)")

	return
}

func getTablesMetaData(db *sql.DB, querySet DialectQuerySet, schemaName, tableType string) ([]MetaData, error) {

	rows, err := db.Query(querySet.ListOfTablesQuery(), schemaName, tableType)

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

		tableInfo, err := GetTableMetaData(db, querySet, schemaName, tableName)

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
