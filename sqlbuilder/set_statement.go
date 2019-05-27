package sqlbuilder

import (
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/sqlbuilder/execution"
)

type setStatement interface {
	Statement
	expression
	hasRows()

	ORDER_BY(clauses ...orderByClause) setStatement
	LIMIT(limit int64) setStatement
	OFFSET(offset int64) setStatement

	AsTable(alias string) expressionTable
}

const (
	union     = "UNION"
	intersect = "INTERSECT"
	except    = "EXCEPT"
)

func UNION(selects ...rowsType) setStatement {
	return newSetStatementImpl(union, false, selects...)
}

func UNION_ALL(selects ...rowsType) setStatement {
	return newSetStatementImpl(union, true, selects...)
}

func INTERSECT(selects ...rowsType) setStatement {
	return newSetStatementImpl(intersect, false, selects...)
}

func INTERSECT_ALL(selects ...rowsType) setStatement {
	return newSetStatementImpl(intersect, true, selects...)
}

func EXCEPT(selects ...rowsType) setStatement {
	return newSetStatementImpl(except, false, selects...)
}

func EXCEPT_ALL(selects ...rowsType) setStatement {
	return newSetStatementImpl(except, true, selects...)
}

// Similar to selectStatementImpl, but less complete
type setStatementImpl struct {
	expressionInterfaceImpl
	isRowsType

	operator      string
	selects       []rowsType
	orderBy       []orderByClause
	limit, offset int64
	// True if results of the union should be deduped.
	all bool
}

func newSetStatementImpl(operator string, all bool, selects ...rowsType) setStatement {
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

func (us *setStatementImpl) ORDER_BY(orderBy ...orderByClause) setStatement {

	us.orderBy = orderBy
	return us
}

func (us *setStatementImpl) LIMIT(limit int64) setStatement {
	us.limit = limit
	return us
}

func (us *setStatementImpl) OFFSET(offset int64) setStatement {
	us.offset = offset
	return us
}

func (us *setStatementImpl) AsTable(alias string) expressionTable {
	return &expressionTableImpl{
		statement: us,
		alias:     alias,
	}
}

func (s *setStatementImpl) serialize(statement statementType, out *queryData) error {
	if s == nil {
		return errors.New("Set statement is nil. ")
	}

	if s.orderBy != nil || s.limit >= 0 || s.offset >= 0 {
		out.writeString("(")
		out.increaseIdent()
	}

	err := s.serializeImpl(out)

	if err != nil {
		return err
	}

	if s.orderBy != nil || s.limit >= 0 || s.offset >= 0 {
		out.decreaseIdent()
		out.nextLine()
		out.writeString(")")
	}

	return nil
}

func (s *setStatementImpl) serializeImpl(out *queryData) error {
	if s == nil {
		return errors.New("Set statement is nil. ")
	}

	if len(s.selects) < 2 {
		return errors.Newf("UNION Statement must have at least two SELECT statements.")
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
		out.insertArgument(s.limit)
	}

	if s.offset >= 0 {
		out.nextLine()
		out.writeString("OFFSET")
		out.insertArgument(s.offset)
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
