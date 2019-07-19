package jet

import (
	"context"
	"database/sql"
	"errors"
	"github.com/go-jet/jet/execution"
)

// TableLockMode is a type of possible SQL table lock
type TableLockMode string

// Lock types for LockStatement.
const (
	LOCK_ACCESS_SHARE           = "ACCESS SHARE"
	LOCK_ROW_SHARE              = "ROW SHARE"
	LOCK_ROW_EXCLUSIVE          = "ROW EXCLUSIVE"
	LOCK_SHARE_UPDATE_EXCLUSIVE = "SHARE UPDATE EXCLUSIVE"
	LOCK_SHARE                  = "SHARE"
	LOCK_SHARE_ROW_EXCLUSIVE    = "SHARE ROW EXCLUSIVE"
	LOCK_EXCLUSIVE              = "EXCLUSIVE"
	LOCK_ACCESS_EXCLUSIVE       = "ACCESS EXCLUSIVE"
)

// LockStatement interface for SQL LOCK statement
type LockStatement interface {
	Statement

	IN(lockMode TableLockMode) LockStatement
	NOWAIT() LockStatement
}

type lockStatementImpl struct {
	tables   []WritableTable
	lockMode TableLockMode
	nowait   bool
}

// LOCK creates lock statement for list of tables.
func LOCK(tables ...WritableTable) LockStatement {
	return &lockStatementImpl{
		tables: tables,
	}
}

func (l *lockStatementImpl) IN(lockMode TableLockMode) LockStatement {
	l.lockMode = lockMode
	return l
}

func (l *lockStatementImpl) NOWAIT() LockStatement {
	l.nowait = true
	return l
}

func (l *lockStatementImpl) DebugSql() (query string, err error) {
	return debugSql(l)
}

func (l *lockStatementImpl) Sql() (query string, args []interface{}, err error) {
	if l == nil {
		return "", nil, errors.New("jet: nil Statement")
	}

	if len(l.tables) == 0 {
		return "", nil, errors.New("jet: There is no table selected to be locked")
	}

	out := &sqlBuilder{}

	out.newLine()
	out.writeString("LOCK TABLE")

	for i, table := range l.tables {
		if i > 0 {
			out.writeString(", ")
		}

		err := table.serialize(lockStatement, out)

		if err != nil {
			return "", nil, err
		}
	}

	if l.lockMode != "" {
		out.writeString("IN")
		out.writeString(string(l.lockMode))
		out.writeString("MODE")
	}

	if l.nowait {
		out.writeString("NOWAIT")
	}

	query, args = out.finalize()
	return
}

func (l *lockStatementImpl) Query(db execution.DB, destination interface{}) error {
	return query(l, db, destination)
}

func (l *lockStatementImpl) QueryContext(context context.Context, db execution.DB, destination interface{}) error {
	return queryContext(context, l, db, destination)
}

func (l *lockStatementImpl) Exec(db execution.DB) (sql.Result, error) {
	return exec(l, db)
}

func (l *lockStatementImpl) ExecContext(context context.Context, db execution.DB) (res sql.Result, err error) {
	return execContext(l, db, context)
}
