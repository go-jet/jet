package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/internal/utils/semantic"
	"github.com/go-jet/jet/v2/qrm"
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

func getTableInfoQuery(db *sql.DB) (string, error) {
	var version string
	err := db.QueryRow("select sqlite_version();").Scan(&version)

	if err != nil {
		return "", fmt.Errorf("failed to get sqlite version: %w", err)
	}

	sqliteVersion, err := semantic.VersionFromString(version)

	if err != nil {
		return "", fmt.Errorf("can't parse sqlite version: %w", err)
	}

	// generated columns were added in version 3.26.0
	if sqliteVersion.Lt(semantic.Version{Major: 3, Minor: 26, Patch: 0}) {
		return `select * from pragma_table_info(?);`, nil
	}

	return `select * from pragma_table_xinfo(?);`, nil
}

func (p sqliteQuerySet) GetTableColumnsMetaData(db *sql.DB, schemaName string, tableName string) ([]metadata.Column, error) {

	tableInfoQuery, err := getTableInfoQuery(db)

	if err != nil {
		return nil, err
	}

	var columnInfos []struct {
		Name      string
		Type      string
		NotNull   int32
		DfltValue string
		Pk        int32
		Hidden    int32
	}

	_, err = qrm.Query(context.Background(), db, tableInfoQuery, []interface{}{tableName}, &columnInfos)
	if err != nil {
		return nil, fmt.Errorf("failed to query '%s' column metadata: %w", tableName, err)
	}

	var columns []metadata.Column

	for _, columnInfo := range columnInfos {
		columnType := strings.TrimSuffix(getColumnType(columnInfo.Type), " GENERATED ALWAYS")
		isGenerated := columnInfo.Hidden == 2 || columnInfo.Hidden == 3 // stored or virtual column
		hasDefault := columnInfo.DfltValue != ""

		columns = append(columns, metadata.Column{
			Name:         columnInfo.Name,
			IsPrimaryKey: columnInfo.Pk != 0,
			IsNullable:   columnInfo.NotNull != 1,
			IsGenerated:  isGenerated,
			HasDefault:   hasDefault,
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
