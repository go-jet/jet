package jet

import (
	"errors"
)

// UNION effectively appends the result of sub-queries(select statements) into single query.
// It eliminates duplicate rows from its result.
func UNION(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return newSetStatementImpl(Union, false, toSelectList(lhs, rhs, selects...))
}

// UNION_ALL effectively appends the result of sub-queries(select statements) into single query.
// It does not eliminates duplicate rows from its result.
func UNION_ALL(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return newSetStatementImpl(Union, true, toSelectList(lhs, rhs, selects...))
}

// INTERSECT returns all rows that are in query results.
// It eliminates duplicate rows from its result.
func INTERSECT(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return newSetStatementImpl(Intersect, false, toSelectList(lhs, rhs, selects...))
}

// INTERSECT_ALL returns all rows that are in query results.
// It does not eliminates duplicate rows from its result.
func INTERSECT_ALL(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return newSetStatementImpl(Intersect, true, toSelectList(lhs, rhs, selects...))
}

// EXCEPT returns all rows that are in the result of query lhs but not in the result of query rhs.
// It eliminates duplicate rows from its result.
func EXCEPT(lhs, rhs SelectStatement) SelectStatement {
	return newSetStatementImpl(Except, false, toSelectList(lhs, rhs))
}

// EXCEPT_ALL returns all rows that are in the result of query lhs but not in the result of query rhs.
// It does not eliminates duplicate rows from its result.
func EXCEPT_ALL(lhs, rhs SelectStatement) SelectStatement {
	return newSetStatementImpl(Except, true, toSelectList(lhs, rhs))
}

func toSelectList(lhs, rhs SelectStatement, selects ...SelectStatement) []SelectStatement {
	return append([]SelectStatement{lhs, rhs}, selects...)
}

const (
	Union     = "UNION"
	Intersect = "INTERSECT"
	Except    = "EXCEPT"
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

	setStatement.selectStatementImpl.ExpressionInterfaceImpl.Parent = setStatement
	setStatement.selectStatementImpl.parent = setStatement
	setStatement.limit = -1
	setStatement.offset = -1

	return setStatement
}

func (s *setStatementImpl) accept(visitor visitor) {
	visitor.visit(s)

	for _, selects := range s.selects {
		selects.accept(visitor)
	}
}

func (s *setStatementImpl) projections() []Projection {
	if len(s.selects) > 0 {
		return s.selects[0].projections()
	}
	return []Projection{}
}

func (s *setStatementImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	if s == nil {
		return errors.New("jet: Set expression is nil. ")
	}

	wrap := s.orderBy != nil || s.limit >= 0 || s.offset >= 0

	if wrap {
		out.WriteString("(")
		out.increaseIdent()
	}

	err := s.serializeImpl(out)

	if err != nil {
		return err
	}

	if wrap {
		out.decreaseIdent()
		out.NewLine()
		out.WriteString(")")
	}

	return nil
}

func (s *setStatementImpl) serializeImpl(out *SqlBuilder) error {
	if s == nil {
		return errors.New("jet: Set expression is nil. ")
	}

	if len(s.selects) < 2 {
		return errors.New("jet: UNION Statement must have at least two SELECT statements")
	}

	if setOverride := out.Dialect.SerializeOverride(s.operator); setOverride != nil {
		return setOverride()(SelectStatementType, out)
	}

	out.NewLine()
	out.WriteString("(")
	out.increaseIdent()

	for i, selectStmt := range s.selects {
		out.NewLine()
		if i > 0 {
			out.WriteString(s.operator)

			if s.all {
				out.WriteString("ALL")
			}
			out.NewLine()
		}

		if selectStmt == nil {
			return errors.New("jet: select statement is nil")
		}

		err := selectStmt.serialize(SetStatementType, out)

		if err != nil {
			return err
		}
	}

	out.decreaseIdent()
	out.NewLine()
	out.WriteString(")")

	if s.orderBy != nil {
		err := out.writeOrderBy(SetStatementType, s.orderBy)
		if err != nil {
			return err
		}
	}

	if s.limit >= 0 {
		out.NewLine()
		out.WriteString("LIMIT")
		out.insertParametrizedArgument(s.limit)
	}

	if s.offset >= 0 {
		out.NewLine()
		out.WriteString("OFFSET")
		out.insertParametrizedArgument(s.offset)
	}

	return nil
}

func (s *setStatementImpl) Sql(dialect ...Dialect) (query string, args []interface{}, err error) {
	queryData := &SqlBuilder{
		Dialect: detectDialect(s, dialect...),
	}

	err = s.serializeImpl(queryData)

	if err != nil {
		return
	}

	query, args = queryData.finalize()
	return
}
