package cubrid

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	cubridgo "github.com/search5/cubrid-go"
	"github.com/go-jet/jet/v2/generator/metadata"
)

// cubridQuerySet implements metadata.DialectQuerySet for CUBRID databases.
//
// Uses cubrid-go's catalog query functions which internally use SQL queries
// against CUBRID's system catalog tables (db_class, db_attribute, db_index).
// The CCI protocol CAS_FC_SCHEMA_INFO cannot be used here because
// KEEP_CONNECTION=AUTO (default) closes the TCP socket between SCHEMA_INFO
// and FETCH, making protocol-based schema introspection unreliable.
type cubridQuerySet struct{}

func (c cubridQuerySet) GetTablesMetaData(db *sql.DB, schemaName string, tableType metadata.TableType) ([]metadata.Table, error) {
	ctx := context.Background()

	var names []string
	var err error

	if tableType == metadata.BaseTable {
		names, err = cubridgo.ListTables(ctx, db)
	} else {
		names, err = cubridgo.ListViews(ctx, db)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list %s names: %w", tableType, err)
	}

	var tables []metadata.Table
	for _, name := range names {
		columns, err := getColumnMetaData(ctx, db, name)
		if err != nil {
			return nil, fmt.Errorf("failed to get columns for %s: %w", name, err)
		}
		tables = append(tables, metadata.Table{
			Name:    name,
			Columns: columns,
		})
	}

	return tables, nil
}

func (c cubridQuerySet) GetEnumsMetaData(db *sql.DB, schemaName string) ([]metadata.Enum, error) {
	// CUBRID does not have standalone enum types. Enum information
	// is embedded in column definitions and mapped as String type.
	return nil, nil
}

// getColumnMetaData uses cubrid-go's ListColumns and ListPrimaryKeys
// to build go-jet Column metadata.
func getColumnMetaData(ctx context.Context, db *sql.DB, tableName string) ([]metadata.Column, error) {
	cols, err := cubridgo.ListColumns(ctx, db, tableName)
	if err != nil {
		return nil, fmt.Errorf("list columns: %w", err)
	}

	pks, err := cubridgo.ListPrimaryKeys(ctx, db, tableName)
	if err != nil {
		return nil, fmt.Errorf("list primary keys: %w", err)
	}
	pkSet := make(map[string]bool, len(pks))
	for _, pk := range pks {
		pkSet[pk] = true
	}

	var columns []metadata.Column
	for _, col := range cols {
		name := strings.TrimSpace(col.Name)
		dataType := strings.TrimSpace(col.DataType)

		columns = append(columns, metadata.Column{
			Name:         name,
			IsPrimaryKey: pkSet[name],
			IsNullable:   col.IsNullable,
			HasDefault:   col.DefaultValue != "" && col.DefaultValue != "NULL",
			DataType: metadata.DataType{
				Name: mapCubridDataType(dataType),
				Kind: dataTypeKind(dataType),
			},
		})
	}

	return columns, nil
}
