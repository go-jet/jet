package sqlite

import (
	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/internal/jet/db"
)

// RawStatement creates new sql statements from raw query and optional map of named arguments
func RawStatement(rawQuery string, namedArguments ...RawArgs) Statement {
	return jet.RawStatement(Dialect, rawQuery, namedArguments...)
}

// DB is a wrapper around sql.DB, adding prepared statement caching capability.
type DB = db.DB

// NewDB creates new DB wrapper with statements caching disabled
var NewDB = db.NewDB

// Tx is a wrapper around *sql.Tx, adding prepared statement caching capability.
type Tx = db.Tx
