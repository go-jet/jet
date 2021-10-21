package metadata

import (
	"database/sql"
	"fmt"
)

// TableType is type of database table(view or base)
type TableType string

// SQL table types
const (
	BaseTable TableType = "BASE TABLE"
	ViewTable TableType = "VIEW"
)

// DialectQuerySet is set of methods necessary to retrieve dialect meta data information
type DialectQuerySet interface {
	GetTablesMetaData(db *sql.DB, schemaName string, tableType TableType) []Table
	GetEnumsMetaData(db *sql.DB, schemaName string) []Enum
}

// GetSchema retrieves Schema information from database
func GetSchema(db *sql.DB, querySet DialectQuerySet, schemaName string) Schema {
	ret := Schema{
		Name:           schemaName,
		TablesMetaData: querySet.GetTablesMetaData(db, schemaName, BaseTable),
		ViewsMetaData:  querySet.GetTablesMetaData(db, schemaName, ViewTable),
		EnumsMetaData:  querySet.GetEnumsMetaData(db, schemaName),
	}

	fmt.Println("	FOUND", len(ret.TablesMetaData), "table(s),", len(ret.ViewsMetaData), "view(s),",
		len(ret.EnumsMetaData), "enum(s)")

	return ret
}
