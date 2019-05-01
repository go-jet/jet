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

type SetStatement interface {
	Statement

	ORDER_BY(clauses ...OrderByClause) SetStatement
	LIMIT(limit int64) SetStatement
	OFFSET(offset int64) SetStatement
}

func UNION(selects ...SelectStatement) SetStatement {
	return newSetStatementImpl(union, false, selects...)
}

func UNION_ALL(selects ...SelectStatement) SetStatement {
	return newSetStatementImpl(union, true, selects...)
}

func INTERSECT(selects ...SelectStatement) SetStatement {
	return newSetStatementImpl(intersect, false, selects...)
}

func INTERSECT_ALL(selects ...SelectStatement) SetStatement {
	return newSetStatementImpl(intersect, true, selects...)
}

func EXCEPT(selects ...SelectStatement) SetStatement {
	return newSetStatementImpl(except, false, selects...)
}

func EXCEPT_ALL(selects ...SelectStatement) SetStatement {
	return newSetStatementImpl(except, true, selects...)
}

// Similar to selectStatementImpl, but less complete
type setStatementImpl struct {
	operator      string
	selects       []SelectStatement
	order         *listClause
	limit, offset int64
	// True if results of the union should be deduped.
	all bool
}

func newSetStatementImpl(operator string, all bool, selects ...SelectStatement) *setStatementImpl {
	return &setStatementImpl{
		operator: operator,
		selects:  selects,
		limit:    -1,
		offset:   -1,
		all:      all,
	}
}

func (us *setStatementImpl) ORDER_BY(clauses ...OrderByClause) SetStatement {

	us.order = newOrderByListClause(clauses...)
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

func (us *setStatementImpl) Serialize(out *queryData, options ...serializeOption) error {
	if len(us.selects) == 0 {
		return errors.Newf("UNION statement must have at least one SELECT")
	}

	out.WriteString("(")

	for i, selectStmt := range us.selects {
		if i > 0 {
			out.WriteString(" " + us.operator + " ")

			if us.all {
				out.WriteString(" ALL ")
			}
		}

		err := selectStmt.Serialize(out, options...)

		if err != nil {
			return err
		}
	}

	out.WriteString(")")

	if us.order != nil {
		out.WriteString(" ORDER BY ")
		if err := us.order.Serialize(out, NO_TABLE_NAME); err != nil {
			return err
		}
	}

	if us.limit >= 0 {
		out.WriteString(" LIMIT ")
		out.InsertArgument(us.limit)
	}

	if us.offset >= 0 {
		out.WriteString(" OFFSET ")
		out.InsertArgument(us.offset)
	}

	return nil
}

func (us *setStatementImpl) Sql() (query string, args []interface{}, err error) {
	queryData := &queryData{}

	err = us.Serialize(queryData)

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
