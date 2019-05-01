package sqlbuilder

import (
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/types"
)

type UnionStatement interface {
	Statement

	ORDER_BY(clauses ...OrderByClause) UnionStatement
	LIMIT(limit int64) UnionStatement
	OFFSET(offset int64) UnionStatement
}

func Union(selects ...SelectStatement) UnionStatement {
	return &unionStatementImpl{
		selects: selects,
		limit:   -1,
		offset:  -1,
		all:     true,
	}
}

func UnionAll(selects ...SelectStatement) UnionStatement {
	return &unionStatementImpl{
		selects: selects,
		limit:   -1,
		offset:  -1,
		all:     false,
	}
}

// Similar to selectStatementImpl, but less complete
type unionStatementImpl struct {
	selects       []SelectStatement
	order         *listClause
	limit, offset int64
	// True if results of the union should be deduped.
	all bool
}

func (us *unionStatementImpl) Serialize(out *queryData, options ...serializeOption) error {
	if len(us.selects) == 0 {
		return errors.Newf("Union statement must have at least one SELECT")
	}

	out.WriteString("(")

	for i, selectStmt := range us.selects {
		if i > 0 {
			out.WriteString(" UNION ")

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

func (us *unionStatementImpl) ORDER_BY(clauses ...OrderByClause) UnionStatement {

	us.order = newOrderByListClause(clauses...)
	return us
}

func (us *unionStatementImpl) LIMIT(limit int64) UnionStatement {
	us.limit = limit
	return us
}

func (us *unionStatementImpl) OFFSET(offset int64) UnionStatement {
	us.offset = offset
	return us
}

func (us *unionStatementImpl) Sql() (query string, args []interface{}, err error) {
	queryData := &queryData{}

	err = us.Serialize(queryData)

	if err != nil {
		return
	}

	return queryData.buff.String(), queryData.args, nil
}

func (s *unionStatementImpl) Query(db types.Db, destination interface{}) error {
	return Query(s, db, destination)
}

func (u *unionStatementImpl) Execute(db types.Db) (res sql.Result, err error) {
	return Execute(u, db)
}
