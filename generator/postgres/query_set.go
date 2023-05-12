package postgres

import (
	"context"
	"database/sql"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/qrm"
)

// postgresQuerySet is dialect query set for PostgreSQL
type postgresQuerySet struct{}

// redshiftQuerySet is dialect query based off PostgreSQL that is compatible with Redshift
type redshiftQuerySet struct{}

func (p postgresQuerySet) GetTablesMetaData(db *sql.DB, schemaName string, tableType metadata.TableType) []metadata.Table {
	query := `
SELECT table_name as "table.name" 
FROM information_schema.tables
WHERE table_schema = $1 and table_type = $2
ORDER BY table_name;
`
	var tables []metadata.Table

	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName, tableType}, &tables)
	throw.OnError(err)

	for i := range tables {
		tables[i].Columns = p.GetTableColumnsMetaData(db, schemaName, tables[i].Name)
	}

	return tables
}

func (p postgresQuerySet) GetTableColumnsMetaData(db *sql.DB, schemaName string, tableName string) []metadata.Column {
	query := `
WITH primaryKeys AS (
	SELECT column_name
	FROM information_schema.key_column_usage AS c
		LEFT JOIN information_schema.table_constraints AS t
			 ON t.constraint_name = c.constraint_name AND 
				c.table_schema = t.table_schema AND 
				c.table_name = t.table_name
	WHERE t.table_schema = $1 AND t.table_name = $2 AND t.constraint_type = 'PRIMARY KEY'
)
SELECT column_name as "column.Name", 
	   is_nullable = 'YES' as "column.isNullable",
	   is_generated = 'ALWAYS' or is_generated = 'YES' as "column.isGenerated",
	   (EXISTS(SELECT 1 from primaryKeys as pk where pk.column_name = columns.column_name)) as "column.IsPrimaryKey",
	   dataType.kind as "dataType.Kind",	
	   (case dataType.Kind when 'base' then data_type else LTRIM(udt_name, '_') end) as "dataType.Name", 
	   FALSE as "dataType.isUnsigned"
FROM information_schema.columns,
	 LATERAL (select (case data_type
				when 'ARRAY' then 'array'
				when 'USER-DEFINED' then 
					case (select t.typtype 
						  from pg_type as t 
						  join pg_namespace as p on p.oid = t.typnamespace 
						  where t.typname = columns.udt_name and p.nspname = $1)
						when 'e' then 'enum'
						else 'user-defined'
					end
				else 'base'
			end) as Kind) as dataType
where table_schema = $1 and table_name = $2
order by ordinal_position;
`
	var columns []metadata.Column
	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName, tableName}, &columns)
	throw.OnError(err)

	return columns
}

func (p postgresQuerySet) GetEnumsMetaData(db *sql.DB, schemaName string) []metadata.Enum {
	query := `
SELECT t.typname as "enum.name",  
       e.enumlabel as "values"
FROM pg_catalog.pg_type t 
   JOIN pg_catalog.pg_enum e on t.oid = e.enumtypid  
   JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = $1
ORDER BY n.nspname, t.typname, e.enumsortorder;`

	var result []metadata.Enum

	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName}, &result)
	throw.OnError(err)

	return result
}

func (r redshiftQuerySet) GetTablesMetaData(db *sql.DB, schemaName string, tableType metadata.TableType) []metadata.Table {
	query := `
SELECT table_name as "table.name" 
FROM information_schema.tables
WHERE table_schema = $1 and table_type = $2
ORDER BY table_name;
`
	var tables []metadata.Table

	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName, tableType}, &tables)
	throw.OnError(err)

	for i := range tables {
		tables[i].Columns = r.GetTableColumnsMetaData(db, schemaName, tables[i].Name)
	}

	return tables
}

func (r redshiftQuerySet) GetTableColumnsMetaData(db *sql.DB, schemaName string, tableName string) []metadata.Column {
	query := `
WITH primaryKeys AS (
	SELECT column_name
	FROM information_schema.key_column_usage AS c
		LEFT JOIN information_schema.table_constraints AS t
			 ON t.constraint_name = c.constraint_name AND
				c.table_schema = t.table_schema AND
				c.table_name = t.table_name
	WHERE t.table_schema = 'reporting' AND t.table_name = 'post_metrics' AND t.constraint_type = 'PRIMARY KEY'
)
SELECT columns.column_name as "column.Name",
	   is_nullable = 'YES' as "column.isNullable",
	   'false' as "column.isGenerated",
	   (pk.column_name IS NOT NULL) as "column.IsPrimaryKey",
	   (CASE columns.data_type
                 WHEN 'ARRAY' THEN 'array'
                 WHEN 'USER-DEFINED' THEN 'user-defined'
                 ELSE 'base'
       END) AS "dataType.Kind",
	   columns.udt_name AS "dataType.Name",
	   FALSE as "dataType.isUnsigned"
FROM information_schema.columns AS columns
LEFT JOIN primaryKeys AS pk
    ON pk.column_name = columns.column_name
WHERE columns.table_schema = 'reporting' AND columns.table_name = 'post_metrics'
ORDER BY columns.ordinal_position;
`
	var columns []metadata.Column
	_, err := qrm.Query(context.Background(), db, query, []interface{}{schemaName, tableName}, &columns)
	throw.OnError(err)

	return columns
}

// GetEnumsMetaData
// redshift does not support enums
func (r redshiftQuerySet) GetEnumsMetaData(db *sql.DB, schemaName string) []metadata.Enum {
	return nil
}
