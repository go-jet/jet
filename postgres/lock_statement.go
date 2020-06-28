package postgres

import "github.com/go-jet/jet/v2/internal/jet"

// TableLockMode is a type of possible SQL table lock
type TableLockMode string

// Lock types for LockStatement.
const (
	LOCK_ACCESS_SHARE           TableLockMode = "ACCESS SHARE"
	LOCK_ROW_SHARE              TableLockMode = "ROW SHARE"
	LOCK_ROW_EXCLUSIVE          TableLockMode = "ROW EXCLUSIVE"
	LOCK_SHARE_UPDATE_EXCLUSIVE TableLockMode = "SHARE UPDATE EXCLUSIVE"
	LOCK_SHARE                  TableLockMode = "SHARE"
	LOCK_SHARE_ROW_EXCLUSIVE    TableLockMode = "SHARE ROW EXCLUSIVE"
	LOCK_EXCLUSIVE              TableLockMode = "EXCLUSIVE"
	LOCK_ACCESS_EXCLUSIVE       TableLockMode = "ACCESS EXCLUSIVE"
)

// LockStatement is interface for MySQL LOCK tables
type LockStatement interface {
	Statement

	IN(lockMode TableLockMode) LockStatement
	NOWAIT() LockStatement
}

// LOCK creates LockStatement from list of tables
func LOCK(tables ...jet.SerializerTable) LockStatement {
	newLock := &lockStatementImpl{}
	newLock.SerializerStatement = jet.NewStatementImpl(Dialect, jet.LockStatementType, newLock,
		&newLock.StatementBegin, &newLock.In, &newLock.NoWait)

	newLock.StatementBegin.Name = "LOCK TABLE"
	newLock.StatementBegin.Tables = tables
	newLock.NoWait.Name = "NOWAIT"
	return newLock
}

type lockStatementImpl struct {
	jet.SerializerStatement

	StatementBegin jet.ClauseStatementBegin
	In             jet.ClauseIn
	NoWait         jet.ClauseOptional
}

func (l *lockStatementImpl) IN(lockMode TableLockMode) LockStatement {
	l.In.LockMode = string(lockMode)
	return l
}

func (l *lockStatementImpl) NOWAIT() LockStatement {
	l.NoWait.Show = true
	return l
}
