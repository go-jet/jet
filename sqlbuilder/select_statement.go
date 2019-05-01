package sqlbuilder

import (
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/types"
)

type SelectStatement interface {
	Statement
	Expression

	DISTINCT() SelectStatement
	WHERE(expression BoolExpression) SelectStatement
	GROUP_BY(expressions ...Clause) SelectStatement
	HAVING(boolExpression BoolExpression) SelectStatement
	ORDER_BY(clauses ...OrderByClause) SelectStatement

	LIMIT(limit int64) SelectStatement
	OFFSET(offset int64) SelectStatement

	FOR_UPDATE() SelectStatement

	AsTable(alias string) *SelectStatementTable
}

// NOTE: SelectStatement purposely does not implement the Table interface since
// mysql's subquery performance is horrible.
type selectStatementImpl struct {
	expressionInterfaceImpl

	table       ReadableTable
	distinct    bool
	projections []Projection
	where       BoolExpression
	groupBy     []Clause
	having      BoolExpression
	orderBy     []OrderByClause

	limit, offset int64

	forUpdate bool
}

func newSelectStatement(
	table ReadableTable,
	projections []Projection) SelectStatement {

	return &selectStatementImpl{
		table:       table,
		projections: projections,
		limit:       -1,
		offset:      -1,
		forUpdate:   false,
		distinct:    false,
	}
}

func (s *selectStatementImpl) Serialize(out *queryData, options ...serializeOption) error {

	out.WriteString("(")

	err := s.serializeImpl(out, options...)

	if err != nil {
		return err
	}

	out.WriteString(")")

	return nil
}

func (s *selectStatementImpl) serializeImpl(out *queryData, options ...serializeOption) error {

	out.WriteString("SELECT ")

	if s.distinct {
		out.WriteString("DISTINCT ")
	}

	if s.projections == nil || len(s.projections) == 0 {
		return errors.New("No column selected for projection.")
	}

	err := serializeProjectionList(s.projections, out)

	if err != nil {
		return err
	}

	out.WriteString(" FROM ")

	if s.table == nil {
		return errors.Newf("nil tableName.")
	}

	if err := s.table.SerializeSql(out); err != nil {
		return err
	}

	if s.where != nil {
		out.WriteString(" WHERE ")
		if err := s.where.Serialize(out); err != nil {
			return err
		}
	}

	if s.groupBy != nil && len(s.groupBy) > 0 {
		out.WriteString(" GROUP BY ")

		err := serializeClauseList(s.groupBy, out)

		if err != nil {
			return err
		}
	}

	if s.having != nil {
		out.WriteString(" HAVING ")
		if err = s.having.Serialize(out); err != nil {
			return err
		}
	}

	if s.orderBy != nil {
		out.WriteString(" ORDER BY ")
		if err := serializeOrderByClauseList(s.orderBy, out); err != nil {
			return err
		}
	}

	if s.limit >= 0 {
		out.WriteString(" LIMIT ")
		out.InsertArgument(s.limit)
	}

	if s.offset >= 0 {
		out.WriteString(" OFFSET ")
		out.InsertArgument(s.offset)
	}

	if s.forUpdate {
		out.WriteString(" FOR UPDATE")
	}

	return nil
}

// Return the properly escaped SQL statement, against the specified database
func (q *selectStatementImpl) Sql() (query string, args []interface{}, err error) {
	queryData := queryData{}

	err = q.serializeImpl(&queryData)

	if err != nil {
		return "", nil, err
	}

	return queryData.buff.String(), queryData.args, nil
}

func (s *selectStatementImpl) AsTable(alias string) *SelectStatementTable {
	return &SelectStatementTable{
		statement: s,
		alias:     alias,
	}
}

func (q *selectStatementImpl) WHERE(expression BoolExpression) SelectStatement {
	q.where = expression
	return q
}

func (s *selectStatementImpl) GROUP_BY(cluases ...Clause) SelectStatement {
	s.groupBy = cluases
	return s
}

func (q *selectStatementImpl) HAVING(expression BoolExpression) SelectStatement {
	q.having = expression
	return q
}

func (q *selectStatementImpl) ORDER_BY(clauses ...OrderByClause) SelectStatement {

	q.orderBy = clauses

	return q
}

func (q *selectStatementImpl) OFFSET(offset int64) SelectStatement {
	q.offset = offset
	return q
}

func (q *selectStatementImpl) LIMIT(limit int64) SelectStatement {
	q.limit = limit
	return q
}

func (q *selectStatementImpl) DISTINCT() SelectStatement {
	q.distinct = true
	return q
}

func (q *selectStatementImpl) FOR_UPDATE() SelectStatement {
	q.forUpdate = true
	return q
}

func (s *selectStatementImpl) Query(db types.Db, destination interface{}) error {
	return Query(s, db, destination)
}

func (u *selectStatementImpl) Execute(db types.Db) (res sql.Result, err error) {
	return Execute(u, db)
}

func NumExp(statement SelectStatement) NumericExpression {
	return newNumericExpressionWrap(statement)
}
