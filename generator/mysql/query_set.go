package mysql

import (
	"context"
	"database/sql"
	"strings"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/qrm"
)

// mySqlQuerySet is dialect query set for MySQL
type mySqlQuerySet struct{}

func (m mySqlQuerySet) GetTablesMetaData(db *sql.DB, schemaName string, tableType metadata.TableType) []metadata.Table {
	query := `
SELECT table_name as "table.name"
FROM INFORMATION_SCHEMA.tables
WHERE table_schema = ? and table_type = ?
ORDER BY table_name;
`
	var tables []metadata.Table

	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName, tableType}, &tables)
	throw.OnError(err)

	for i := range tables {
		tables[i].Columns = m.GetTableColumnsMetaData(db, schemaName, tables[i].Name)
	}

	return tables
}

func (m mySqlQuerySet) GetTableColumnsMetaData(db *sql.DB, schemaName string, tableName string) []metadata.Column {
	query := `
SELECT COLUMN_NAME AS "column.Name", 
	IS_NULLABLE = "YES" AS "column.IsNullable", 
	(EXISTS(
		SELECT 1
		FROM information_schema.table_constraints t
			JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name)
		WHERE table_schema = ? AND table_name = ? AND t.constraint_type='PRIMARY KEY' AND k.column_name = columns.column_name
	)) AS "column.IsPrimaryKey",
	IF (COLUMN_TYPE = 'tinyint(1)', 
			'boolean', 
			IF (DATA_TYPE='enum', 
					CONCAT(TABLE_NAME, '_', COLUMN_NAME), 
					DATA_TYPE)
	) AS "dataType.Name", 
	IF (DATA_TYPE = 'enum', 'enum', 'base') AS "dataType.Kind", 
	COLUMN_TYPE LIKE '%unsigned%' AS "dataType.IsUnsigned"
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position;
`
	var columns []metadata.Column
	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName, tableName, schemaName, tableName}, &columns)
	throw.OnError(err)

	return columns
}

func (m *mySqlQuerySet) GetEnumsMetaData(db *sql.DB, schemaName string) []metadata.Enum {
	query := `
SELECT (CASE c.DATA_TYPE WHEN 'enum' then CONCAT(c.TABLE_NAME, '_', c.COLUMN_NAME) ELSE '' END ) as "name", 
       SUBSTRING(c.COLUMN_TYPE,5) as "values"
FROM information_schema.columns as c
	INNER JOIN information_schema.tables  as t on (t.table_schema = c.table_schema AND t.table_name = c.table_name)
WHERE c.table_schema = ? AND DATA_TYPE = 'enum';
`
	var queryResult []struct {
		Name   string
		Values string
	}

	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName}, &queryResult)
	throw.OnError(err)

	var ret []metadata.Enum

	for _, result := range queryResult {
		enumValues := strings.Replace(result.Values[1:len(result.Values)-1], "'", "", -1)

		ret = append(ret, metadata.Enum{
			Name:   result.Name,
			Values: strings.Split(enumValues, ","),
		})
	}

	return ret
}
