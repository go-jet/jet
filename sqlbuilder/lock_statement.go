package sqlbuilder

import (
	"database/sql"
	"github.com/pkg/errors"
	"github.com/sub0zero/go-sqlbuilder/types"
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

type lockStatement interface {
	Statement

	IN(lockMode lockMode) lockStatement
	NOWAIT() lockStatement
}

type lockStatementImpl struct {
	tables   []tableInterface
	lockMode lockMode
	nowait   bool
}

func LOCK(tables ...tableInterface) lockStatement {
	return &lockStatementImpl{
		tables: tables,
	}
}

func (l *lockStatementImpl) IN(lockMode lockMode) lockStatement {
	l.lockMode = lockMode
	return l
}

func (l *lockStatementImpl) NOWAIT() lockStatement {
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

	out.nextLine()
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

func (l *lockStatementImpl) Query(db types.Db, destination interface{}) error {
	return Query(l, db, destination)
}

func (l *lockStatementImpl) Execute(db types.Db) (sql.Result, error) {
	return Execute(l, db)
}
