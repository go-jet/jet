package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/qrm"
)

// postgresQuerySet is dialect query set for PostgreSQL
type postgresQuerySet struct{}

func (p postgresQuerySet) GetTablesMetaData(db *sql.DB, schemaName string, tableType metadata.TableType) ([]metadata.Table, error) {
	query := `
SELECT table_name as "table.name", obj_description((quote_ident(table_schema)||'.'||quote_ident(table_name))::regclass) as "table.comment"
FROM information_schema.tables
WHERE table_schema = $1 and table_type = $2
ORDER BY table_name;
`
	var tables []metadata.Table

	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName, tableType}, &tables)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s metadata: %w", tableType, err)
	}

	// add materialized views separately, because materialized views are not part of standard information schema
	if tableType == metadata.ViewTable {
		matViewQuery := `
			select matviewname as "table.name"
			from pg_matviews
			where schemaname = $1;
		`
		var matViews []metadata.Table

		_, err := qrm.Query(context.Background(), db, matViewQuery, []interface{}{schemaName}, &matViews)
		if err != nil {
			return nil, fmt.Errorf("failed to query materialized view metadata: %w", err)
		}

		tables = append(tables, matViews...)
	}

	for i := range tables {
		tables[i].Columns, err = getColumnsMetaData(db, schemaName, tables[i].Name)
		if err != nil {
			return nil, fmt.Errorf("failed to query %s columns metadata: %w", tableType, err)
		}
	}

	return tables, nil
}

func getColumnsMetaData(db *sql.DB, schemaName string, tableName string) ([]metadata.Column, error) {
	query := `
select  
    attr.attname as "column.Name",
    col_description(attr.attrelid, attr.attnum) as "column.Comment",
    exists(
        select 1
        from pg_catalog.pg_index indx
        where attr.attrelid = indx.indrelid and attr.attnum = any(indx.indkey) and indx.indisprimary
    ) as "column.IsPrimaryKey",
    not attr.attnotnull as "column.isNullable",
    attr.attgenerated = 's' as "column.isGenerated",
    attr.atthasdef as "column.hasDefault",
    (case
        when tp.typtype = 'b' AND tp.typcategory <> 'A' then 'base'
        when tp.typtype = 'b' AND tp.typcategory = 'A' then 'array'
        when tp.typtype = 'd' then 'base'
        when tp.typtype = 'e' then 'enum'
        when tp.typtype = 'r' then 'range'
     end) as "dataType.Kind",
    (case when tp.typtype = 'd' then (select pg_type.typname from pg_catalog.pg_type where pg_type.oid = tp.typbasetype)
          when tp.typcategory = 'A' then pg_catalog.format_type(attr.atttypid, attr.atttypmod)
          else tp.typname
     end) as "dataType.Name",
    false as "dataType.isUnsigned"
from pg_catalog.pg_attribute as attr
     join pg_catalog.pg_class as cls on cls.oid = attr.attrelid
     join pg_catalog.pg_namespace as ns on ns.oid = cls.relnamespace
     join pg_catalog.pg_type as tp on tp.oid = attr.atttypid
where 
    ns.nspname = $1 and
    cls.relname = $2 and 
    not attr.attisdropped and 
    attr.attnum > 0
order by 
    attr.attnum;
`
	var columns []metadata.Column
	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName, tableName}, &columns)
	if err != nil {
		return nil, fmt.Errorf("failed to query '%s' columns metadata: %w", tableName, err)
	}

	return columns, nil
}

func (p postgresQuerySet) GetEnumsMetaData(db *sql.DB, schemaName string) ([]metadata.Enum, error) {
	query := `
SELECT t.typname as "enum.name",  
	   obj_description(t.oid) as "enum.comment",
       e.enumlabel as "values"
FROM pg_catalog.pg_type t 
   JOIN pg_catalog.pg_enum e on t.oid = e.enumtypid  
   JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = $1
ORDER BY n.nspname, t.typname, e.enumsortorder;`

	var result []metadata.Enum

	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName}, &result)
	if err != nil {
		return nil, fmt.Errorf("failed to query enums metadata for schema '%s': %w", schemaName, err)
	}

	return result, nil
}
