package sqlbuilder

import (
	"database/sql"
	"github.com/go-jet/jet/sqlbuilder/execution"
	"github.com/pkg/errors"
)

type lockMode string

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

type LockStatement interface {
	Statement

	IN(lockMode lockMode) LockStatement
	NOWAIT() LockStatement
}

type lockStatementImpl struct {
	tables   []WritableTable
	lockMode lockMode
	nowait   bool
}

func LOCK(tables ...WritableTable) LockStatement {
	return &lockStatementImpl{
		tables: tables,
	}
}

func (l *lockStatementImpl) IN(lockMode lockMode) LockStatement {
	l.lockMode = lockMode
	return l
}

func (l *lockStatementImpl) NOWAIT() LockStatement {
	l.nowait = true
	return l
}

func (l *lockStatementImpl) DebugSql() (query string, err error) {
	return DebugSql(l)
}

func (l *lockStatementImpl) Sql() (query string, args []interface{}, err error) {
	if l == nil {
		return "", nil, errors.New("nil Statement.")
	}

	if len(l.tables) == 0 {
		return "", nil, errors.New("There is no table selected to be locked. ")
	}

	out := &queryData{}

	out.newLine()
	out.writeString("LOCK TABLE")

	for i, table := range l.tables {
		if i > 0 {
			out.writeString(", ")
		}

		err := table.serialize(lock_statement, out)

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

func (l *lockStatementImpl) Query(db execution.Db, destination interface{}) error {
	return Query(l, db, destination)
}

func (l *lockStatementImpl) Exec(db execution.Db) (sql.Result, error) {
	return Exec(l, db)
}
