package jet

import (
	"errors"
)

func UNION(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return newSetStatementImpl(union, false, toSelectList(lhs, rhs, selects...))
}

func UNION_ALL(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return newSetStatementImpl(union, true, toSelectList(lhs, rhs, selects...))
}

func INTERSECT(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return newSetStatementImpl(intersect, false, toSelectList(lhs, rhs, selects...))
}

func INTERSECT_ALL(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return newSetStatementImpl(intersect, true, toSelectList(lhs, rhs, selects...))
}

func EXCEPT(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return newSetStatementImpl(except, false, toSelectList(lhs, rhs, selects...))
}

func EXCEPT_ALL(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return newSetStatementImpl(except, true, toSelectList(lhs, rhs, selects...))
}

func toSelectList(lhs, rhs SelectStatement, selects ...SelectStatement) []SelectStatement {
	return append([]SelectStatement{lhs, rhs}, selects...)
}

const (
	union     = "UNION"
	intersect = "INTERSECT"
	except    = "EXCEPT"
)

// Similar to selectStatementImpl, but less complete
type setStatementImpl struct {
	selectStatementImpl

	operator string
	all      bool
	selects  []SelectStatement
}

func newSetStatementImpl(operator string, all bool, selects []SelectStatement) SelectStatement {
	setStatement := &setStatementImpl{
		operator: operator,
		all:      all,
		selects:  selects,
	}

	setStatement.selectStatementImpl.expressionInterfaceImpl.parent = setStatement
	setStatement.selectStatementImpl.parent = setStatement
	setStatement.limit = -1
	setStatement.offset = -1

	return setStatement
}

func (s *setStatementImpl) projections() []projection {
	if len(s.selects) > 0 {
		return s.selects[0].projections()
	}
	return []projection{}
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

		if selectStmt == nil {
			return errors.New("select statement is nil")
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
