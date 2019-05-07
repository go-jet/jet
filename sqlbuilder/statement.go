package sqlbuilder

import (
	"database/sql"
	"github.com/sub0zero/go-sqlbuilder/types"
)

type statement interface {
	// String returns generated SQL as string.
	Sql() (query string, args []interface{}, err error)

	Query(db types.Db, destination interface{}) error
	Execute(db types.Db) (sql.Result, error)
}
