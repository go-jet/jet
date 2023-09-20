package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/qrm"
	"golang.org/x/sync/errgroup"
)

// mySqlQuerySet is dialect query set for MySQL
type mySqlQuerySet struct{}

func (m mySqlQuerySet) GetTablesMetaData(db *sql.DB, schemaName string, tableType metadata.TableType) ([]metadata.Table, error) {
	query := `
SELECT table_name as "table.name"
FROM INFORMATION_SCHEMA.tables
WHERE table_schema = ? and table_type = ?
ORDER BY table_name;
`
	var tables []metadata.Table

	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName, tableType}, &tables)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s metadata result: %w", tableType, err)
	}

	wg := errgroup.Group{}
	for i := 0; i < len(tables); i++ {
		i := i
		wg.Go(func() (err1 error) {
			tables[i].Columns, err1 = m.GetTableColumnsMetaData(db, schemaName, tables[i].Name)
			return err1
		})
	}

	err = wg.Wait()
	return tables, err
}

func (m mySqlQuerySet) GetTableColumnsMetaData(db *sql.DB, schemaName string, tableName string) ([]metadata.Column, error) {
	query := `
SELECT
		col.COLUMN_NAME AS "column.Name",
		col.IS_NULLABLE = "YES" AS "column.IsNullable",
		col.COLUMN_COMMENT AS "column.Comment",
		COALESCE(pk.IsPrimaryKey, 0) AS "column.IsPrimaryKey",
		IF (col.COLUMN_TYPE = 'tinyint(1)',
				'boolean',
				IF (col.DATA_TYPE = 'enum',
						CONCAT(col.TABLE_NAME, '_', col.COLUMN_NAME),
						col.DATA_TYPE)
		) AS "dataType.Name",
		IF (col.DATA_TYPE = 'enum', 'enum', 'base') AS "dataType.Kind",
		col.COLUMN_TYPE LIKE '%unsigned%' AS "dataType.IsUnsigned"
FROM
		information_schema.columns AS col
LEFT JOIN (
		SELECT k.column_name, 1 AS IsPrimaryKey
		FROM information_schema.table_constraints t
		JOIN information_schema.key_column_usage k USING(constraint_name, table_schema, table_name)
		WHERE t.table_schema =  ?
			AND t.table_name = ?
			AND t.constraint_type = 'PRIMARY KEY'
) AS pk ON col.COLUMN_NAME = pk.column_name
WHERE
		col.table_schema = ?
		AND col.table_name = ?
ORDER BY
		col.ordinal_position;
	`

	var columns []metadata.Column
	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName, tableName, schemaName, tableName}, &columns)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s column meta data: %w", tableName, err)
	}

	return columns, nil
}

func (m mySqlQuerySet) GetEnumsMetaData(db *sql.DB, schemaName string) ([]metadata.Enum, error) {
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
	if err != nil {
		return nil, fmt.Errorf("failed to query enums meta data: %w", err)
	}

	var ret []metadata.Enum

	for _, result := range queryResult {
		enumValues := strings.Replace(result.Values[1:len(result.Values)-1], "'", "", -1)

		ret = append(ret, metadata.Enum{
			Name:   result.Name,
			Values: strings.Split(enumValues, ","),
		})
	}

	return ret, nil
}
