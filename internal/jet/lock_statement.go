package jet

import (
	"context"
	"database/sql"
	"errors"
	"github.com/go-jet/jet/execution"
)

// TableLockMode is a type of possible SQL table lock
type TableLockMode string

// LockStatement interface for SQL LOCK statement
type LockStatement interface {
	Statement

	IN(lockMode string) LockStatement
	NOWAIT() LockStatement
}

type lockStatementImpl struct {
	tables   []WritableTable
	lockMode string
	nowait   bool
}

// LOCK creates lock statement for list of tables.
func LOCK(tables ...WritableTable) LockStatement {
	return &lockStatementImpl{
		tables: tables,
	}
}

func (l *lockStatementImpl) IN(lockMode string) LockStatement {
	l.lockMode = lockMode
	return l
}

func (l *lockStatementImpl) NOWAIT() LockStatement {
	l.nowait = true
	return l
}

func (l *lockStatementImpl) DebugSql(dialect ...Dialect) (query string, err error) {
	return debugSql(l, dialect...)
}

func (l *lockStatementImpl) accept(visitor visitor) {
	visitor.visit(l)

	for _, table := range l.tables {
		table.accept(visitor)
	}
}

func (l *lockStatementImpl) Sql(dialect ...Dialect) (query string, args []interface{}, err error) {
	if l == nil {
		return "", nil, errors.New("jet: nil Statement")
	}

	if len(l.tables) == 0 {
		return "", nil, errors.New("jet: There is no table selected to be locked")
	}

	out := &SqlBuilder{
		Dialect: detectDialect(l, dialect...),
	}

	out.NewLine()
	out.WriteString("LOCK TABLE")

	for i, table := range l.tables {
		if i > 0 {
			out.WriteString(", ")
		}

		err := table.serialize(LockStatementType, out)

		if err != nil {
			return "", nil, err
		}
	}

	if l.lockMode != "" {
		out.WriteString("IN")
		out.WriteString(string(l.lockMode))
		out.WriteString("MODE")
	}

	if l.nowait {
		out.WriteString("NOWAIT")
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
	return execContext(context, l, db)
}
