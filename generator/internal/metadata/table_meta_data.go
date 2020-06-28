package metadata

import (
	"database/sql"
	"github.com/go-jet/jet/v2/internal/utils"
	"strings"
)

// TableMetaData metadata struct
type TableMetaData struct {
	SchemaName  string
	name        string
	PrimaryKeys map[string]bool
	Columns     []ColumnMetaData
}

// Name returns table info name
func (t TableMetaData) Name() string {
	return t.name
}

// IsPrimaryKey returns if column is a part of primary key
func (t TableMetaData) IsPrimaryKey(column string) bool {
	return t.PrimaryKeys[column]
}

// MutableColumns returns list of mutable columns for table
func (t TableMetaData) MutableColumns() []ColumnMetaData {
	ret := []ColumnMetaData{}

	for _, column := range t.Columns {
		if t.IsPrimaryKey(column.Name) {
			continue
		}

		ret = append(ret, column)
	}

	return ret
}

// GetImports returns model imports for table.
func (t TableMetaData) GetImports() []string {
	imports := map[string]string{}

	for _, column := range t.Columns {
		columnType := column.GoBaseType

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
func (t TableMetaData) GoStructName() string {
	return utils.ToGoIdentifier(t.name) + "Table"
}

// GoStructImplName returns go struct impl name for sql builder
func (t TableMetaData) GoStructImplName() string {
	name := utils.ToGoIdentifier(t.name) + "Table"
	return string(strings.ToLower(name)[0]) + name[1:]
}

// GetTableMetaData returns table info metadata
func GetTableMetaData(db *sql.DB, querySet DialectQuerySet, schemaName, tableName string) (tableInfo TableMetaData) {
	tableInfo.SchemaName = schemaName
	tableInfo.name = tableName

	tableInfo.PrimaryKeys = getPrimaryKeys(db, querySet, schemaName, tableName)
	tableInfo.Columns = getColumnsMetaData(db, querySet, schemaName, tableName)
	return
}

func getPrimaryKeys(db *sql.DB, querySet DialectQuerySet, schemaName, tableName string) map[string]bool {

	rows, err := db.Query(querySet.PrimaryKeysQuery(), schemaName, tableName)
	utils.PanicOnError(err)

	primaryKeyMap := map[string]bool{}

	for rows.Next() {
		primaryKey := ""
		err := rows.Scan(&primaryKey)
		utils.PanicOnError(err)

		primaryKeyMap[primaryKey] = true
	}

	return primaryKeyMap
}
