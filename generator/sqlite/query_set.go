package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/qrm"
	"strings"
)

// sqliteQuerySet is dialect query set for SQLite
type sqliteQuerySet struct{}

func (p sqliteQuerySet) GetTablesMetaData(db *sql.DB, schemaName string, tableType metadata.TableType) ([]metadata.Table, error) {
	query := `
	SELECT name as "table.name" 
	FROM sqlite_master
	WHERE type=? AND name != 'sqlite_sequence'
	ORDER BY name;
`
	sqlTableType := "table"

	if tableType == metadata.ViewTable {
		sqlTableType = "view"
	}

	var tables []metadata.Table

	_, err := qrm.Query(context.Background(), db, query, []interface{}{sqlTableType}, &tables)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s metadata: %w", schemaName, err)
	}

	for i := range tables {
		tables[i].Columns, err = p.GetTableColumnsMetaData(db, schemaName, tables[i].Name)
		if err != nil {
			return nil, fmt.Errorf("failed to query column metadata: %w", err)
		}
	}

	return tables, nil
}

func (p sqliteQuerySet) GetTableColumnsMetaData(db *sql.DB, schemaName string, tableName string) ([]metadata.Column, error) {
	query := fmt.Sprintf(`select * from pragma_table_info(?);`)
	var columnInfos []struct {
		Name    string
		Type    string
		NotNull int32
		Pk      int32
	}

	_, err := qrm.Query(context.Background(), db, query, []interface{}{tableName}, &columnInfos)
	if err != nil {
		return nil, fmt.Errorf("failed to query '%s' column metadata: %w", tableName, err)
	}

	var columns []metadata.Column

	for _, columnInfo := range columnInfos {
		columnType := getColumnType(columnInfo.Type)

		columns = append(columns, metadata.Column{
			Name:         columnInfo.Name,
			IsPrimaryKey: columnInfo.Pk != 0,
			IsNullable:   columnInfo.NotNull != 1,
			DataType: metadata.DataType{
				Name:       columnType,
				Kind:       metadata.BaseType,
				IsUnsigned: false,
			},
		})
	}

	return columns, nil
}

// will convert VARCHAR(10) -> VARCHAR, etc...
func getColumnType(columnType string) string {
	return strings.TrimSpace(strings.Split(columnType, "(")[0])
}

func (p sqliteQuerySet) GetEnumsMetaData(db *sql.DB, schemaName string) ([]metadata.Enum, error) {
	return nil, nil
}
