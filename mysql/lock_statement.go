package mysql

import "github.com/go-jet/jet/v2/internal/jet"

// LockStatement is interface for MySQL LOCK tables
type LockStatement interface {
	Statement
	READ() Statement
	WRITE() Statement
}

// LOCK creates LockStatement from list of tables
func LOCK(tables ...jet.SerializerTable) LockStatement {
	newLock := &lockStatementImpl{
		Lock:  jet.ClauseStatementBegin{Name: "LOCK TABLES", Tables: tables},
		Read:  jet.ClauseOptional{Name: "READ"},
		Write: jet.ClauseOptional{Name: "WRITE"},
	}

	newLock.SerializerStatement = jet.NewStatementImpl(Dialect, jet.LockStatementType, newLock, &newLock.Lock, &newLock.Read, &newLock.Write)

	return newLock
}

type lockStatementImpl struct {
	jet.SerializerStatement

	Lock  jet.ClauseStatementBegin
	Read  jet.ClauseOptional
	Write jet.ClauseOptional
}

func (l *lockStatementImpl) READ() Statement {
	l.Read.Show = true
	return l
}

func (l *lockStatementImpl) WRITE() Statement {
	l.Write.Show = true
	return l
}

// UNLOCK_TABLES explicitly releases any table locks held by the current session
func UNLOCK_TABLES() Statement {
	newUnlock := &unlockStatementImpl{
		Unlock: jet.ClauseStatementBegin{Name: "UNLOCK TABLES"},
	}

	newUnlock.SerializerStatement = jet.NewStatementImpl(Dialect, jet.UnLockStatementType, newUnlock, &newUnlock.Unlock)

	return newUnlock
}

type unlockStatementImpl struct {
	jet.SerializerStatement
	Unlock jet.ClauseStatementBegin
}
