package sqlbuilder

import (
	"bytes"
	"database/sql"
	"github.com/sub0zero/go-sqlbuilder/types"
	"regexp"

	"github.com/dropbox/godropbox/errors"
)

type Statement interface {
	// String returns generated SQL as string.
	String() (sql string, err error)

	Query(db types.Db, destination interface{}) error
	Execute(db types.Db) (sql.Result, error)
}

// LockStatement is used to take Read/Write lock on tables.
// See http://dev.mysql.com/doc/refman/5.0/en/lock-tables.html
//type LockStatement interface {
//	Statement
//
//	AddReadLock(table *Table) LockStatement
//	AddWriteLock(table *Table) LockStatement
//}

//// UnlockStatement can be used to release tableName locks taken using LockStatement.
//// NOTE: You can not selectively release a lock and continue to hold lock on
//// another tableName. UnlockStatement releases all the lock held in the current
//// session.
//type UnlockStatement interface {
//	Statement
//}
//
//// SetGtidNextStatement returns a SQL statement that can be used to explicitly set the next GTID.
//type GtidNextStatement interface {
//	Statement
//}
//
////
//// UNION SELECT Statement ======================================================
////
////
//// LOCK statement ===========================================================
////
//
//// NewLockStatement returns a SQL representing empty set of locks. You need to use
//// AddReadLock/AddWriteLock to add tables that need to be locked.
//// NOTE: You need at least one lock in the set for it to be a valid statement.
//func NewLockStatement() LockStatement {
//	return &lockStatementImpl{}
//}
//
//type lockStatementImpl struct {
//	locks []tableLock
//}
//
//type tableLock struct {
//	t *Table
//	w bool
//}
//
//func (l *lockStatementImpl) Execute(db *sql.DB, data interface{}) error {
//	return nil
//}
//
//// AddReadLock takes read lock on the tableName.
//func (s *lockStatementImpl) AddReadLock(t *Table) LockStatement {
//	s.locks = append(s.locks, tableLock{t: t, w: false})
//	return s
//}
//
//// AddWriteLock takes write lock on the tableName.
//func (s *lockStatementImpl) AddWriteLock(t *Table) LockStatement {
//	s.locks = append(s.locks, tableLock{t: t, w: true})
//	return s
//}
//
//func (s *lockStatementImpl) String() (sql string, err error) {
//	if len(s.locks) == 0 {
//		return "", errors.New("No locks added")
//	}
//
//	buf := new(bytes.Buffer)
//	_, _ = buf.WriteString("LOCK TABLES ")
//
//	for idx, lock := range s.locks {
//		if lock.t == nil {
//			return "", errors.Newf("nil tableName.  Generated sql: %s", buf.String())
//		}
//
//		if err = lock.t.SerializeSql(buf); err != nil {
//			return
//		}
//
//		if lock.w {
//			_, _ = buf.WriteString(" WRITE")
//		} else {
//			_, _ = buf.WriteString(" READ")
//		}
//
//		if idx != len(s.locks)-1 {
//			_, _ = buf.WriteString(", ")
//		}
//	}
//
//	return buf.String(), nil
//}
//
//// NewUnlockStatement returns SQL statement that can be used to release tableName locks
//// grabbed by the current session.
//func NewUnlockStatement() UnlockStatement {
//	return &unlockStatementImpl{}
//}
//
//type unlockStatementImpl struct {
//}
//
//func (u *unlockStatementImpl) Execute(db *sql.DB, data interface{}) error {
//	return nil
//}
//
//func (s *unlockStatementImpl) String() (sql string, err error) {
//	return "UNLOCK TABLES", nil
//}
//
//// SET GTID_NEXT statement returns a SQL statement that can be used to explicitly set the next GTID.
//func NewGtidNextStatement(sid []byte, gno uint64) GtidNextStatement {
//	return &gtidNextStatementImpl{
//		sid: sid,
//		gno: gno,
//	}
//}
//
//type gtidNextStatementImpl struct {
//	sid []byte
//	gno uint64
//}
//
//func (g *gtidNextStatementImpl) Execute(db *sql.DB, data interface{}) error {
//	return nil
//}
//
//func (s *gtidNextStatementImpl) String() (sql string, err error) {
//	// This statement sets a session local variable defining what the next transaction ID is.  It
//	// does not interact with other MySQL sessions. It is neither a DDL nor DML statement, so we
//	// don't have to worry about data corruption.
//	// Because of the string formatting (hex plus an integer), can't morph into another statement.
//	// See: https://dev.mysql.com/doc/refman/5.7/en/replication-options-gtids.html
//	const gtidFormatString = "SET GTID_NEXT=\"%x-%x-%x-%x-%x:%d\""
//
//	buf := new(bytes.Buffer)
//	_, _ = buf.WriteString(fmt.Sprintf(gtidFormatString,
//		s.sid[:4], s.sid[4:6], s.sid[6:8], s.sid[8:10], s.sid[10:], s.gno))
//	return buf.String(), nil
//}

//
// Util functions =============================================================
//

// Once again, teisenberger is lazy.  Here's a quick filter on comments
var validCommentRegexp *regexp.Regexp = regexp.MustCompile("^[\\w .?]*$")

func isValidComment(comment string) bool {
	return validCommentRegexp.MatchString(comment)
}

func writeComment(comment string, buf *bytes.Buffer) error {
	if comment != "" {
		_, _ = buf.WriteString("/* ")
		if !isValidComment(comment) {
			return errors.Newf("Invalid comment: %s", comment)
		}
		_, _ = buf.WriteString(comment)
		_, _ = buf.WriteString(" */")
	}
	return nil
}

func newOrderByListClause(clauses ...OrderByClause) *listClause {
	ret := &listClause{
		clauses:            make([]Clause, len(clauses), len(clauses)),
		includeParentheses: false,
	}

	for i, c := range clauses {
		ret.clauses[i] = c
	}

	return ret
}
