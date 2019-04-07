package metadata

import (
	"database/sql"
	"fmt"
	"github.com/serenize/snaker"
	"strings"
)

type TableInfo struct {
	Name            string
	PrimaryKeys     []string
	ForeignTableMap map[string]string
	Columns         []ColumnInfo
	DatabaseInfo    *DatabaseInfo
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
		case "types.JSONText":
			imports["types.JSONText"] = "github.com/sub0zero/go-sqlbuilder/types"
		}
	}

	ret := []string{}

	for _, packageImport := range imports {
		ret = append(ret, packageImport)
	}

	return ret
}

func (t TableInfo) IsForeignKey(columnName string) bool {
	_, exist := t.ForeignTableMap[columnName]

	return exist
}

func (t TableInfo) ToGoModelStructName() string {
	return snaker.SnakeToCamel(t.Name)
}

func (t TableInfo) ToGoVarName() string {
	return snaker.SnakeToCamel(t.Name)
}

func (t TableInfo) ToGoStructName() string {
	return snaker.SnakeToCamel(t.Name) + "Table"
}

func (t TableInfo) ToGoColumnFieldList(sep string) string {
	columnNames := []string{}
	for _, columnInfo := range t.Columns {
		columnNames = append(columnNames, columnInfo.ToGoVarName())
	}
	return strings.Join(columnNames, sep)
}

func fetchTableInfos(db *sql.DB, databaseInfo *DatabaseInfo) ([]TableInfo, error) {
	query := `
SELECT table_name 
FROM information_schema.tables
where table_schema = $1 and table_type = 'BASE TABLE';`

	//fmt.Println(query, schemaName)

	rows, err := db.Query(query, &databaseInfo.SchemaName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tableInfos := []TableInfo{}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}

		tableInfo := &TableInfo{}
		tableInfo.Name = tableName
		tableInfo.PrimaryKeys, err = getPrimaryKeys(db, databaseInfo.SchemaName, tableName)
		if err != nil {
			return nil, err
		}
		tableInfo.DatabaseInfo = databaseInfo
		tableInfo.Columns, err = fetchColumnInfos(db, tableInfo)

		if err != nil {
			return nil, err
		}

		tableInfo.ForeignTableMap, err = getForignKeyMap(db, databaseInfo.SchemaName, tableName)

		if err != nil {
			return nil, err
		}

		tableInfos = append(tableInfos, *tableInfo)
	}

	fmt.Println("FOUND", len(tableInfos), "tables")

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return tableInfos, nil
}

func getPrimaryKeys(db *sql.DB, schemaName, tableName string) ([]string, error) {
	query := `
SELECT c.column_name
FROM information_schema.key_column_usage AS c
LEFT JOIN information_schema.table_constraints AS t
ON t.constraint_name = c.constraint_name
WHERE t.table_schema = $1 AND t.table_name = $2 AND t.constraint_type = 'PRIMARY KEY';
`
	rows, err := db.Query(query, schemaName, tableName)

	if err != nil {
		return nil, err
	}

	primaryKeys := []string{}

	for rows.Next() {
		primaryKey := ""
		err := rows.Scan(&primaryKey)

		if err != nil {
			return nil, err
		}

		primaryKeys = append(primaryKeys, primaryKey)
	}

	return primaryKeys, nil
}

func getForignKeyMap(db *sql.DB, schemaName, tableName string) (map[string]string, error) {
	query := `
SELECT 
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM 
    information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = $1 AND tc.table_name=$2;
`
	rows, err := db.Query(query, schemaName, tableName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ret := map[string]string{}
	for rows.Next() {
		var columnName, foreignTableName, foreignColumnName string
		err := rows.Scan(&columnName, &foreignTableName, &foreignColumnName)

		if err != nil {
			return nil, err
		}

		ret[columnName] = foreignTableName
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return ret, nil
}
