package metadata

import (
	"database/sql"
)

// DialectQuerySet is set of methods necessary to retrieve dialect meta data information
type DialectQuerySet interface {
	ListOfTablesQuery() string
	PrimaryKeysQuery() string
	ListOfColumnsQuery() string
	ListOfEnumsQuery() string

	GetEnumsMetaData(db *sql.DB, schemaName string) []MetaData
}
