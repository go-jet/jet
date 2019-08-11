package postgres

import "github.com/go-jet/jet/internal/jet"

// UNION effectively appends the result of sub-queries(select statements) into single query.
// It eliminates duplicate rows from its result.
func UNION(lhs, rhs jet.StatementWithProjections, selects ...jet.StatementWithProjections) SetStatement {
	return newSetStatementImpl(Union, false, toSelectList(lhs, rhs, selects...))
}

// UNION_ALL effectively appends the result of sub-queries(select statements) into single query.
// It does not eliminates duplicate rows from its result.
func UNION_ALL(lhs, rhs jet.StatementWithProjections, selects ...jet.StatementWithProjections) SetStatement {
	return newSetStatementImpl(Union, true, toSelectList(lhs, rhs, selects...))
}

// INTERSECT returns all rows that are in query results.
// It eliminates duplicate rows from its result.
func INTERSECT(lhs, rhs jet.StatementWithProjections, selects ...jet.StatementWithProjections) SetStatement {
	return newSetStatementImpl(Intersect, false, toSelectList(lhs, rhs, selects...))
}

// INTERSECT_ALL returns all rows that are in query results.
// It does not eliminates duplicate rows from its result.
func INTERSECT_ALL(lhs, rhs jet.StatementWithProjections, selects ...jet.StatementWithProjections) SetStatement {
	return newSetStatementImpl(Intersect, true, toSelectList(lhs, rhs, selects...))
}

// EXCEPT returns all rows that are in the result of query lhs but not in the result of query rhs.
// It eliminates duplicate rows from its result.
func EXCEPT(lhs, rhs jet.StatementWithProjections) SetStatement {
	return newSetStatementImpl(Except, false, toSelectList(lhs, rhs))
}

// EXCEPT_ALL returns all rows that are in the result of query lhs but not in the result of query rhs.
// It does not eliminates duplicate rows from its result.
func EXCEPT_ALL(lhs, rhs jet.StatementWithProjections) SetStatement {
	return newSetStatementImpl(Except, true, toSelectList(lhs, rhs))
}

type SetStatement interface {
	SetOperators

	ORDER_BY(orderByClauses ...jet.OrderByClause) SetStatement

	LIMIT(limit int64) SetStatement
	OFFSET(offset int64) SetStatement

	AsTable(alias string) SelectTable
}

type SetOperators interface {
	jet.Statement
	jet.HasProjections
	jet.IExpression

	UNION(rhs SelectStatement) SetStatement
	UNION_ALL(rhs SelectStatement) SetStatement
	INTERSECT(rhs SelectStatement) SetStatement
	INTERSECT_ALL(rhs SelectStatement) SetStatement
	EXCEPT(rhs SelectStatement) SetStatement
	EXCEPT_ALL(rhs SelectStatement) SetStatement
}

type setOperatorsImpl struct {
	parent SetOperators
}

func (s *setOperatorsImpl) UNION(rhs SelectStatement) SetStatement {
	return UNION(s.parent, rhs)
}

func (s *setOperatorsImpl) UNION_ALL(rhs SelectStatement) SetStatement {
	return UNION_ALL(s.parent, rhs)
}

func (s *setOperatorsImpl) INTERSECT(rhs SelectStatement) SetStatement {
	return INTERSECT(s.parent, rhs)
}

func (s *setOperatorsImpl) INTERSECT_ALL(rhs SelectStatement) SetStatement {
	return INTERSECT_ALL(s.parent, rhs)
}

func (s *setOperatorsImpl) EXCEPT(rhs SelectStatement) SetStatement {
	return EXCEPT(s.parent, rhs)
}

func (s *setOperatorsImpl) EXCEPT_ALL(rhs SelectStatement) SetStatement {
	return EXCEPT_ALL(s.parent, rhs)
}

type setStatementImpl struct {
	jet.ExpressionStatementImpl

	setOperatorsImpl

	setOperator jet.ClauseSetStmtOperator
}

func newSetStatementImpl(operator string, all bool, selects []jet.StatementWithProjections) SetStatement {
	newSetStatement := &setStatementImpl{}
	newSetStatement.ExpressionStatementImpl.StatementImpl = jet.NewStatementImpl(Dialect, jet.SetStatementType, newSetStatement,
		&newSetStatement.setOperator)
	newSetStatement.ExpressionStatementImpl.ExpressionInterfaceImpl.Parent = newSetStatement

	newSetStatement.setOperator.Operator = operator
	newSetStatement.setOperator.All = all
	newSetStatement.setOperator.Selects = selects
	newSetStatement.setOperator.Limit.Count = -1
	newSetStatement.setOperator.Offset.Count = -1

	newSetStatement.setOperatorsImpl.parent = newSetStatement

	newSetStatement.Clauses = []jet.Clause{&newSetStatement.setOperator}

	return newSetStatement
}

func (s *setStatementImpl) ORDER_BY(orderByClauses ...jet.OrderByClause) SetStatement {
	s.setOperator.OrderBy.List = orderByClauses
	return s
}

func (s *setStatementImpl) LIMIT(limit int64) SetStatement {
	s.setOperator.Limit.Count = limit
	return s
}

func (s *setStatementImpl) OFFSET(offset int64) SetStatement {
	s.setOperator.Offset.Count = offset
	return s
}

func (s *setStatementImpl) AsTable(alias string) SelectTable {
	return newSelectTable(s, alias)
}

const (
	Union     = "UNION"
	Intersect = "INTERSECT"
	Except    = "EXCEPT"
)

func toSelectList(lhs, rhs jet.StatementWithProjections, selects ...jet.StatementWithProjections) []jet.StatementWithProjections {
	return append([]jet.StatementWithProjections{lhs, rhs}, selects...)
}
