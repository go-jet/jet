package sqlbuilder

import (
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/types"
)

const (
	union     = "UNION"
	intersect = "INTERSECT"
	except    = "EXCEPT"
)

type setStatement interface {
	statement
	expression

	ORDER_BY(clauses ...orderByClause) setStatement
	LIMIT(limit int64) setStatement
	OFFSET(offset int64) setStatement

	AsTable(alias string) expressionTable
}

func UNION(selects ...selectStatement) setStatement {
	return newSetStatementImpl(union, false, selects...)
}

func UNION_ALL(selects ...selectStatement) setStatement {
	return newSetStatementImpl(union, true, selects...)
}

func INTERSECT(selects ...selectStatement) setStatement {
	return newSetStatementImpl(intersect, false, selects...)
}

func INTERSECT_ALL(selects ...selectStatement) setStatement {
	return newSetStatementImpl(intersect, true, selects...)
}

func EXCEPT(selects ...selectStatement) setStatement {
	return newSetStatementImpl(except, false, selects...)
}

func EXCEPT_ALL(selects ...selectStatement) setStatement {
	return newSetStatementImpl(except, true, selects...)
}

// Similar to selectStatementImpl, but less complete
type setStatementImpl struct {
	expressionInterfaceImpl

	operator      string
	selects       []selectStatement
	orderBy       []orderByClause
	limit, offset int64
	// True if results of the union should be deduped.
	all bool
}

func newSetStatementImpl(operator string, all bool, selects ...selectStatement) setStatement {
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
	if s.orderBy != nil || s.limit >= 0 || s.offset >= 0 {
		out.writeString("(")
	}

	err := s.serializeImpl(out)

	if err != nil {
		return err
	}

	if s.orderBy != nil || s.limit >= 0 || s.offset >= 0 {
		out.writeString(")")
	}

	return nil
}

func (s *setStatementImpl) serializeImpl(out *queryData) error {

	if len(s.selects) < 2 {
		return errors.Newf("UNION statement must have at least two SELECT statements.")
	}

	out.writeString("(")

	for i, selectStmt := range s.selects {
		if i > 0 {
			out.writeString(" " + s.operator + " ")

			if s.all {
				out.writeString(" ALL ")
			}
		}

		err := selectStmt.serialize(set_statement, out)

		if err != nil {
			return err
		}
	}

	out.writeString(")")

	if s.orderBy != nil {
		err := out.writeOrderBy(set_statement, s.orderBy)
		if err != nil {
			return err
		}
	}

	if s.limit >= 0 {
		out.writeString(" LIMIT ")
		out.insertArgument(s.limit)
	}

	if s.offset >= 0 {
		out.writeString(" OFFSET ")
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

	return queryData.buff.String(), queryData.args, nil
}

func (s *setStatementImpl) Query(db types.Db, destination interface{}) error {
	return Query(s, db, destination)
}

func (u *setStatementImpl) Execute(db types.Db) (res sql.Result, err error) {
	return Execute(u, db)
}
