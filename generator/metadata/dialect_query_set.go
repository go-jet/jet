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

// DialectQuerySet is set of methods necessary to retrieve dialect metadata information
type DialectQuerySet interface {
	GetTablesMetaData(db *sql.DB, schemaName string, tableType TableType) ([]Table, error)
	GetEnumsMetaData(db *sql.DB, schemaName string) ([]Enum, error)
}

// GetSchema retrieves Schema information from database
func GetSchema(db *sql.DB, querySet DialectQuerySet, schemaName string) (Schema, error) {
	tablesMetaData, err := querySet.GetTablesMetaData(db, schemaName, BaseTable)
	if err != nil {
		return Schema{}, fmt.Errorf("failed to get %s tables metadata: %w", schemaName, err)
	}

	viewMetaData, err := querySet.GetTablesMetaData(db, schemaName, ViewTable)
	if err != nil {
		return Schema{}, fmt.Errorf("failed to get %s view metadata: %w", schemaName, err)
	}

	enumsMetaData, err := querySet.GetEnumsMetaData(db, schemaName)
	if err != nil {
		return Schema{}, fmt.Errorf("failed to get %s enum metadata: %w", schemaName, err)
	}

	ret := Schema{
		Name:           schemaName,
		TablesMetaData: tablesMetaData,
		ViewsMetaData:  viewMetaData,
		EnumsMetaData:  enumsMetaData,
	}

	fmt.Println("	FOUND", len(ret.TablesMetaData), "table(s),", len(ret.ViewsMetaData), "view(s),",
		len(ret.EnumsMetaData), "enum(s)")

	return ret, nil
}
