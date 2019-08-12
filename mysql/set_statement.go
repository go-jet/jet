package mysql

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

type SetStatement interface {
	SetOperators

	ORDER_BY(orderByClauses ...jet.OrderByClause) SetStatement

	LIMIT(limit int64) SetStatement
	OFFSET(offset int64) SetStatement

	AsTable(alias string) SelectTable
}

type SetStatementFinal interface {
}

type SetOperators interface {
	jet.Statement
	jet.HasProjections
	jet.Expression

	UNION(rhs SelectStatement) SetStatement
	UNION_ALL(rhs SelectStatement) SetStatement
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
	Union = "UNION"
)

func toSelectList(lhs, rhs jet.StatementWithProjections, selects ...jet.StatementWithProjections) []jet.StatementWithProjections {
	return append([]jet.StatementWithProjections{lhs, rhs}, selects...)
}
