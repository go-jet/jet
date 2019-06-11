package sqlbuilder

import (
	"database/sql"
	"errors"
	"github.com/go-jet/jet/sqlbuilder/execution"
)

type SetStatement interface {
	Statement
	Expression
	hasRows()

	ORDER_BY(clauses ...OrderByClause) SetStatement
	LIMIT(limit int64) SetStatement
	OFFSET(offset int64) SetStatement

	AsTable(alias string) ExpressionTable
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
	isRowsType

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

func (us *setStatementImpl) ORDER_BY(orderBy ...OrderByClause) SetStatement {
	us.orderBy = orderBy
	return us
}

func (us *setStatementImpl) LIMIT(limit int64) SetStatement {
	us.limit = limit
	return us
}

func (us *setStatementImpl) OFFSET(offset int64) SetStatement {
	us.offset = offset
	return us
}

func (us *setStatementImpl) AsTable(alias string) ExpressionTable {
	return newExpressionTable(us.parent, alias)
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
		out.nextLine()
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

	out.nextLine()
	out.writeString("(")
	out.increaseIdent()

	for i, selectStmt := range s.selects {
		out.nextLine()
		if i > 0 {
			out.writeString(s.operator)

			if s.all {
				out.writeString("ALL")
			}
			out.nextLine()
		}

		err := selectStmt.serialize(set_statement, out)

		if err != nil {
			return err
		}
	}

	out.decreaseIdent()
	out.nextLine()
	out.writeString(")")

	if s.orderBy != nil {
		err := out.writeOrderBy(set_statement, s.orderBy)
		if err != nil {
			return err
		}
	}

	if s.limit >= 0 {
		out.nextLine()
		out.writeString("LIMIT")
		out.insertPreparedArgument(s.limit)
	}

	if s.offset >= 0 {
		out.nextLine()
		out.writeString("OFFSET")
		out.insertPreparedArgument(s.offset)
	}

	return nil
}

func (us *setStatementImpl) Sql() (query string, args []interface{}, err error) {
	queryData := &queryData{}

	err = us.serializeImpl(queryData)

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

func (u *setStatementImpl) Execute(db execution.Db) (res sql.Result, err error) {
	return Execute(u, db)
}
