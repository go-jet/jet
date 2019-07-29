package metadata

import (
	"database/sql"
	"strings"
)

type MetaDataQuerySet interface {
	ListOfTablesQuery() string
	PrimaryKeysQuery() string
	ListOfColumnsQuery() string
	ListOfEnumsQuery() string

	GetEnumsMetaData(db *sql.DB, schemaName string) ([]MetaData, error)
}

type PostgresQuerySet struct{}

func (p *PostgresQuerySet) ListOfTablesQuery() string {
	return `
SELECT table_name 
FROM information_schema.tables
where table_schema = $1 and table_type = 'BASE TABLE';
`
}

func (p *PostgresQuerySet) PrimaryKeysQuery() string {
	return `
SELECT c.column_name
FROM information_schema.key_column_usage AS c
LEFT JOIN information_schema.table_constraints AS t
ON t.constraint_name = c.constraint_name
WHERE t.table_schema = $1 AND t.table_name = $2 AND t.constraint_type = 'PRIMARY KEY';
`
}

func (p *PostgresQuerySet) ListOfColumnsQuery() string {
	return `
SELECT column_name, is_nullable, data_type, udt_name, FALSE
FROM information_schema.columns
where table_schema = $1 and table_name = $2
order by ordinal_position;`
}

func (p *PostgresQuerySet) ListOfEnumsQuery() string {
	return `
SELECT t.typname,  
       e.enumlabel
FROM pg_catalog.pg_type t 
   JOIN pg_catalog.pg_enum e on t.oid = e.enumtypid  
   JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = $1
ORDER BY n.nspname, t.typname, e.enumsortorder;`
}

func (p *PostgresQuerySet) GetEnumsMetaData(db *sql.DB, schemaName string) ([]MetaData, error) {
	return getEnumInfos(db, p, schemaName)
}

// =======================================================================//

type MySqlQuerySet struct{}

func (m *MySqlQuerySet) ListOfTablesQuery() string {
	return `
SELECT table_name
FROM INFORMATION_SCHEMA.tables
WHERE table_schema = ? and table_type = 'BASE TABLE';
`
}

func (m *MySqlQuerySet) PrimaryKeysQuery() string {
	return `
SELECT k.column_name
FROM information_schema.table_constraints t
JOIN information_schema.key_column_usage k
USING(constraint_name,table_schema,table_name)
WHERE t.constraint_type='PRIMARY KEY'
  AND t.table_schema= ?
  AND t.table_name= ?;
`
}

func (m *MySqlQuerySet) ListOfColumnsQuery() string {
	return `
SELECT COLUMN_NAME, 
	IS_NULLABLE, IF(COLUMN_TYPE = 'tinyint(1)', 'boolean', DATA_TYPE), 
	IF(DATA_TYPE = 'enum',  CONCAT(TABLE_NAME, '_', COLUMN_NAME), ''), 
	COLUMN_TYPE LIKE '%unsigned%'
FROM information_schema.columns 
WHERE table_schema = ? and table_name = ?
ORDER BY ordinal_position;
`
}

func (m *MySqlQuerySet) ListOfEnumsQuery() string {
	return `
SELECT (CASE DATA_TYPE WHEN 'enum' then CONCAT(TABLE_NAME, '_', COLUMN_NAME) ELSE '' END ), SUBSTRING(COLUMN_TYPE,5)
FROM information_schema.columns 
WHERE table_schema = ?
AND DATA_TYPE = 'enum';
`
}

func (m *MySqlQuerySet) GetEnumsMetaData(db *sql.DB, schemaName string) ([]MetaData, error) {

	rows, err := db.Query(m.ListOfEnumsQuery(), schemaName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ret := []MetaData{}

	for rows.Next() {
		var enumName string
		var enumValues string
		err = rows.Scan(&enumName, &enumValues)
		if err != nil {
			return nil, err
		}

		enumValues = strings.Replace(enumValues[1:len(enumValues)-1], "'", "", -1)

		ret = append(ret, EnumInfo{
			name:   enumName,
			Values: strings.Split(enumValues, ","),
		})
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return ret, nil

}
