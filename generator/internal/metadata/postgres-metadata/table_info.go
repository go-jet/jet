package postgres_metadata

import (
	"database/sql"
	"github.com/serenize/snaker"
)

type TableInfo struct {
	SchemaName  string
	name        string
	PrimaryKeys map[string]bool
	Columns     []ColumnInfo
}

func (t TableInfo) Name() string {
	return t.name
}

func (t TableInfo) IsUnique(columnName string) bool {
	return t.PrimaryKeys[columnName]
}

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

func (t TableInfo) GoStructName() string {
	return snaker.SnakeToCamel(t.name) + "Table"
}

func GetTableInfo(db *sql.DB, dbName, schemaName, tableName string) (tableInfo TableInfo, err error) {

	tableInfo.SchemaName = schemaName
	tableInfo.name = tableName

	tableInfo.PrimaryKeys, err = getPrimaryKeys(db, dbName, schemaName, tableName)
	if err != nil {
		return
	}

	tableInfo.Columns, err = getColumnInfos(db, dbName, schemaName, tableName)

	if err != nil {
		return
	}

	return
}

func getPrimaryKeys(db *sql.DB, dbName, schemaName, tableName string) (map[string]bool, error) {
	query := `
SELECT c.column_name
FROM information_schema.key_column_usage AS c
LEFT JOIN information_schema.table_constraints AS t
ON t.constraint_name = c.constraint_name
WHERE t.table_catalog = $1 AND t.table_schema = $2 AND t.table_name = $3 AND t.constraint_type = 'PRIMARY KEY';
`
	rows, err := db.Query(query, dbName, schemaName, tableName)

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
