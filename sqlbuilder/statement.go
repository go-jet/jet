package sqlbuilder

import (
	"database/sql"
	"github.com/sub0zero/go-sqlbuilder/types"
)

type Statement interface {
	// String returns generated SQL as string.
	Sql() (query string, args []interface{}, err error)

	Query(db types.Db, destination interface{}) error
	Execute(db types.Db) (sql.Result, error)
}

//// SetGtidNextStatement returns a SQL statement that can be used to explicitly set the next GTID.
//type GtidNextStatement interface {
//	Statement
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
//var validCommentRegexp *regexp.Regexp = regexp.MustCompile("^[\\w .?]*$")
//
//func isValidComment(comment string) bool {
//	return validCommentRegexp.MatchString(comment)
//}
//
//func writeComment(comment string, buf *bytes.Buffer) error {
//	if comment != "" {
//		_, _ = buf.WriteString("/* ")
//		if !isValidComment(comment) {
//			return errors.Newf("Invalid comment: %s", comment)
//		}
//		_, _ = buf.WriteString(comment)
//		_, _ = buf.WriteString(" */")
//	}
//	return nil
//}
