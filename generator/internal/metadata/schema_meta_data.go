package metadata

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/internal/utils"
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
func GetSchemaMetaData(db *sql.DB, schemaName string, querySet DialectQuerySet) (schemaInfo SchemaMetaData) {

	schemaInfo.TablesMetaData = getTablesMetaData(db, querySet, schemaName, baseTable)
	schemaInfo.ViewsMetaData = getTablesMetaData(db, querySet, schemaName, view)
	schemaInfo.EnumsMetaData = querySet.GetEnumsMetaData(db, schemaName)

	fmt.Println("	FOUND", len(schemaInfo.TablesMetaData), "table(s),", len(schemaInfo.ViewsMetaData), "view(s),",
		len(schemaInfo.EnumsMetaData), "enum(s)")

	return
}

func getTablesMetaData(db *sql.DB, querySet DialectQuerySet, schemaName, tableType string) []MetaData {

	rows, err := db.Query(querySet.ListOfTablesQuery(), schemaName, tableType)
	utils.PanicOnError(err)
	defer rows.Close()

	ret := []MetaData{}
	for rows.Next() {
		var tableName string

		err = rows.Scan(&tableName)
		utils.PanicOnError(err)

		tableInfo := GetTableMetaData(db, querySet, schemaName, tableName)

		ret = append(ret, tableInfo)
	}

	err = rows.Err()
	utils.PanicOnError(err)

	return ret
}
