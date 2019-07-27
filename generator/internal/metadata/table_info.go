package metadata

import (
	"database/sql"
	"github.com/go-jet/jet/internal/utils"
)

// TableInfo metadata struct
type TableInfo struct {
	SchemaName  string
	name        string
	PrimaryKeys map[string]bool
	Columns     []ColumnInfo
}

// Name returns table info name
func (t TableInfo) Name() string {
	return t.name
}

// IsPrimaryKey returns if column is a part of primary key
func (t TableInfo) IsPrimaryKey(column string) bool {
	return t.PrimaryKeys[column]
}

// MutableColumns returns list of mutable columns for table
func (t TableInfo) MutableColumns() []ColumnInfo {
	ret := []ColumnInfo{}

	for _, column := range t.Columns {
		if t.IsPrimaryKey(column.Name) {
			continue
		}

		ret = append(ret, column)
	}

	return ret
}

// GetImports returns model imports for table.
func (t TableInfo) GetImports() []string {
	imports := map[string]string{}

	for _, column := range t.Columns {
		columnType := column.GoBaseType()

		switch columnType {
		case "time.Time":
			imports["time.Time"] = "time"
		case "uuid.UUID":
			imports["uuid.UUID"] = "github.com/google/uuid"
		}
	}

	ret := []string{}

	for _, packageImport := range imports {
		ret = append(ret, packageImport)
	}

	return ret
}

// GoStructName returns go struct name for sql builder
func (t TableInfo) GoStructName() string {
	return utils.ToGoIdentifier(t.name) + "Table"
}

// GetTableInfo returns table info metadata
func GetTableInfo(db *sql.DB, querySet MetaDataQuerySet, schemaName, tableName string) (tableInfo TableInfo, err error) {

	tableInfo.SchemaName = schemaName
	tableInfo.name = tableName

	tableInfo.PrimaryKeys, err = getPrimaryKeys(db, querySet, schemaName, tableName)
	if err != nil {
		return
	}

	tableInfo.Columns, err = getColumnInfos(db, querySet, schemaName, tableName)

	if err != nil {
		return
	}

	return
}

func getPrimaryKeys(db *sql.DB, querySet MetaDataQuerySet, schemaName, tableName string) (map[string]bool, error) {

	rows, err := db.Query(querySet.PrimaryKeysQuery(), schemaName, tableName)

	if err != nil {
		return nil, err
	}

	primaryKeyMap := map[string]bool{}

	for rows.Next() {
		primaryKey := ""
		err := rows.Scan(&primaryKey)

		if err != nil {
			return nil, err
		}

		primaryKeyMap[primaryKey] = true
	}

	return primaryKeyMap, nil
}
