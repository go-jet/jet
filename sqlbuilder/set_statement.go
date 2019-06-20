package sqlbuilder

import (
	"context"
	"database/sql"
	"errors"
	"github.com/go-jet/jet/sqlbuilder/execution"
)

type SetStatement interface {
	Statement
	Expression

	ORDER_BY(clauses ...OrderByClause) SetStatement
	LIMIT(limit int64) SetStatement
	OFFSET(offset int64) SetStatement

	AsTable(alias string) ExpressionTable

	projections() []projection
}

const (
	union     = "UNION"
	intersect = "INTERSECT"
	except    = "EXCEPT"
)

func UNION(selects ...rowsType) SetStatement {
	return newSetStatementImpl(union, false, selects...)
}

func UNION_ALL(selects ...rowsType) SetStatement {
	return newSetStatementImpl(union, true, selects...)
}

func INTERSECT(selects ...rowsType) SetStatement {
	return newSetStatementImpl(intersect, false, selects...)
}

func INTERSECT_ALL(selects ...rowsType) SetStatement {
	return newSetStatementImpl(intersect, true, selects...)
}

func EXCEPT(selects ...rowsType) SetStatement {
	return newSetStatementImpl(except, false, selects...)
}

func EXCEPT_ALL(selects ...rowsType) SetStatement {
	return newSetStatementImpl(except, true, selects...)
}

// Similar to selectStatementImpl, but less complete
type setStatementImpl struct {
	expressionInterfaceImpl

	operator      string
	selects       []rowsType
	orderBy       []OrderByClause
	limit, offset int64

	all bool
}

func newSetStatementImpl(operator string, all bool, selects ...rowsType) SetStatement {
	setStatement := &setStatementImpl{
		operator: operator,
		selects:  selects,
		limit:    -1,
		offset:   -1,
		all:      all,
	}

	setStatement.expressionInterfaceImpl.parent = setStatement

	return setStatement
}

func (s *setStatementImpl) ORDER_BY(orderBy ...OrderByClause) SetStatement {
	s.orderBy = orderBy
	return s
}

func (s *setStatementImpl) LIMIT(limit int64) SetStatement {
	s.limit = limit
	return s
}

func (s *setStatementImpl) OFFSET(offset int64) SetStatement {
	s.offset = offset
	return s
}

func (s *setStatementImpl) projections() []projection {
	if len(s.selects) > 0 {
		return s.selects[0].projections()
	}
	return []projection{}
}

func (s *setStatementImpl) AsTable(alias string) ExpressionTable {
	return newExpressionTable(s.parent, alias, s.projections())
}

func (s *setStatementImpl) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	if s == nil {
		return errors.New("Set expression is nil. ")
	}

	wrap := s.orderBy != nil || s.limit >= 0 || s.offset >= 0

	if wrap {
		out.writeString("(")
		out.increaseIdent()
	}

	err := s.serializeImpl(out)

	if err != nil {
		return err
	}

	if wrap {
		out.decreaseIdent()
		out.newLine()
		out.writeString(")")
	}

	return nil
}

func (s *setStatementImpl) serializeImpl(out *queryData) error {
	if s == nil {
		return errors.New("Set expression is nil. ")
	}

	if len(s.selects) < 2 {
		return errors.New("UNION Statement must have at least two SELECT statements.")
	}

	out.newLine()
	out.writeString("(")
	out.increaseIdent()

	for i, selectStmt := range s.selects {
		out.newLine()
		if i > 0 {
			out.writeString(s.operator)

			if s.all {
				out.writeString("ALL")
			}
			out.newLine()
		}

		err := selectStmt.serialize(set_statement, out)

		if err != nil {
			return err
		}
	}

	out.decreaseIdent()
	out.newLine()
	out.writeString(")")

	if s.orderBy != nil {
		err := out.writeOrderBy(set_statement, s.orderBy)
		if err != nil {
			return err
		}
	}

	if s.limit >= 0 {
		out.newLine()
		out.writeString("LIMIT")
		out.insertPreparedArgument(s.limit)
	}

	if s.offset >= 0 {
		out.newLine()
		out.writeString("OFFSET")
		out.insertPreparedArgument(s.offset)
	}

	return nil
}

func (s *setStatementImpl) Sql() (query string, args []interface{}, err error) {
	queryData := &queryData{}

	err = s.serializeImpl(queryData)

	if err != nil {
		return
	}

	query, args = queryData.finalize()
	return
}

func (s *setStatementImpl) DebugSql() (query string, err error) {
	return DebugSql(s)
}

func (s *setStatementImpl) Query(db execution.Db, destination interface{}) error {
	return Query(s, db, destination)
}

func (s *setStatementImpl) QueryContext(db execution.Db, context context.Context, destination interface{}) error {
	return QueryContext(s, db, context, destination)
}

func (s *setStatementImpl) Exec(db execution.Db) (res sql.Result, err error) {
	return Exec(s, db)
}

func (s *setStatementImpl) ExecContext(db execution.Db, context context.Context) (res sql.Result, err error) {
	return ExecContext(s, db, context)
}
