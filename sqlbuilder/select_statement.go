package sqlbuilder

import (
	"bytes"
	"fmt"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/sqlbuilder/execution"
	"github.com/sub0zero/go-sqlbuilder/types"
	"reflect"
)

type SelectStatement interface {
	Statement
	Expression

	Where(expression BoolExpression) SelectStatement
	AndWhere(expression BoolExpression) SelectStatement
	GroupBy(expressions ...Expression) SelectStatement
	HAVING(expressions BoolExpression) SelectStatement

	OrderBy(clauses ...OrderByClause) SelectStatement
	Limit(limit int64) SelectStatement
	Offset(offset int64) SelectStatement
	Distinct() SelectStatement
	WithSharedLock() SelectStatement
	ForUpdate() SelectStatement
	Comment(comment string) SelectStatement
	Copy() SelectStatement

	AsTable(alias string) *SelectStatementTable

	Execute(db types.Db, destination interface{}) error
	//ExecuteInTx(tx *sql.Tx, destination interface{}) error
}

// NOTE: SelectStatement purposely does not implement the Table interface since
// mysql's subquery performance is horrible.
type selectStatementImpl struct {
	expressionInterfaceImpl

	table          ReadableTable
	projections    []Projection
	where          BoolExpression
	group          *listClause
	having         BoolExpression
	order          *listClause
	comment        string
	limit, offset  int64
	withSharedLock bool
	forUpdate      bool
	distinct       bool
}

func newSelectStatement(
	table ReadableTable,
	projections []Projection) SelectStatement {

	return &selectStatementImpl{
		table:          table,
		projections:    projections,
		limit:          -1,
		offset:         -1,
		withSharedLock: false,
		forUpdate:      false,
		distinct:       false,
	}
}

func (s *selectStatementImpl) SerializeSql(out *bytes.Buffer, options ...serializeOption) error {
	str, err := s.String()

	if err != nil {
		return err
	}

	out.WriteString("(")
	out.WriteString(str)
	out.WriteString(")")

	return nil
}

func (s *selectStatementImpl) AsTable(alias string) *SelectStatementTable {
	return &SelectStatementTable{
		statement: s,
		alias:     alias,
	}
}

func (s *selectStatementImpl) Execute(db types.Db, destination interface{}) error {
	destinationType := reflect.TypeOf(destination)

	if destinationType.Kind() == reflect.Ptr && destinationType.Elem().Kind() == reflect.Struct {
		s.Limit(1)
	}

	query, err := s.String()

	if err != nil {
		return err
	}

	return execution.Execute(db, query, destination)
}

func (s *selectStatementImpl) Copy() SelectStatement {
	ret := *s
	return &ret
}

// Further filter the query, instead of replacing the filter
func (q *selectStatementImpl) AndWhere(
	expression BoolExpression) SelectStatement {

	if q.where == nil {
		return q.Where(expression)
	}
	q.where = And(q.where, expression)
	return q
}

func (q *selectStatementImpl) Where(expression BoolExpression) SelectStatement {
	q.where = expression
	return q
}

func (q *selectStatementImpl) GroupBy(
	expressions ...Expression) SelectStatement {

	q.group = &listClause{
		clauses:            make([]Clause, len(expressions), len(expressions)),
		includeParentheses: false,
	}

	for i, e := range expressions {
		q.group.clauses[i] = e
	}
	return q
}

func (q *selectStatementImpl) HAVING(expression BoolExpression) SelectStatement {
	q.having = expression
	return q
}

func (q *selectStatementImpl) OrderBy(
	clauses ...OrderByClause) SelectStatement {

	q.order = newOrderByListClause(clauses...)
	return q
}

func (q *selectStatementImpl) Limit(limit int64) SelectStatement {
	q.limit = limit
	return q
}

func (q *selectStatementImpl) Distinct() SelectStatement {
	q.distinct = true
	return q
}

func (q *selectStatementImpl) WithSharedLock() SelectStatement {
	// We don't need to grab a read lock if we're going to grab a write one
	if !q.forUpdate {
		q.withSharedLock = true
	}
	return q
}

func (q *selectStatementImpl) ForUpdate() SelectStatement {
	// Clear a request for a shared lock if we're asking for a write one
	q.withSharedLock = false
	q.forUpdate = true
	return q
}

func (q *selectStatementImpl) Offset(offset int64) SelectStatement {
	q.offset = offset
	return q
}

func (q *selectStatementImpl) Comment(comment string) SelectStatement {
	q.comment = comment
	return q
}

// Return the properly escaped SQL statement, against the specified database
func (q *selectStatementImpl) String() (sql string, err error) {
	buf := new(bytes.Buffer)
	_, _ = buf.WriteString("SELECT ")

	if err = writeComment(q.comment, buf); err != nil {
		return
	}

	if q.distinct {
		_, _ = buf.WriteString("DISTINCT ")
	}

	if q.projections == nil || len(q.projections) == 0 {
		return "", errors.Newf(
			"No column selected.  Generated sql: %s",
			buf.String())
	}

	for i, col := range q.projections {
		if i > 0 {
			_ = buf.WriteByte(',')
		}
		if col == nil {
			return "", errors.Newf(
				"nil column selected.  Generated sql: %s",
				buf.String())
		}
		if err = col.SerializeForProjection(buf); err != nil {
			return
		}
	}

	_, _ = buf.WriteString(" FROM ")
	if q.table == nil {
		return "", errors.Newf("nil tableName.  Generated sql: %s", buf.String())
	}
	if err = q.table.SerializeSql(buf); err != nil {
		return
	}

	if q.where != nil {
		_, _ = buf.WriteString(" WHERE ")
		if err = q.where.SerializeSql(buf); err != nil {
			return
		}
	}

	if q.group != nil {
		_, _ = buf.WriteString(" GROUP BY ")
		if err = q.group.SerializeSql(buf); err != nil {
			return
		}
	}

	if q.having != nil {
		buf.WriteString(" HAVING ")
		if err = q.having.SerializeSql(buf); err != nil {
			return
		}
	}

	if q.order != nil {
		_, _ = buf.WriteString(" ORDER BY ")
		if err = q.order.SerializeSql(buf); err != nil {
			return
		}
	}

	if q.limit >= 0 {
		if q.offset >= 0 {
			_, _ = buf.WriteString(fmt.Sprintf(" LIMIT %d, %d", q.offset, q.limit))
		} else {
			_, _ = buf.WriteString(fmt.Sprintf(" LIMIT %d", q.limit))
		}
	}

	if q.forUpdate {
		_, _ = buf.WriteString(" FOR UPDATE")
	} else if q.withSharedLock {
		_, _ = buf.WriteString(" LOCK IN SHARE MODE")
	}

	return buf.String(), nil
}

func NumExp(statement SelectStatement) NumericExpression {
	return newNumericExpressionWrap(statement)
}
