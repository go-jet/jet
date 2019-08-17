package mysql

import "github.com/go-jet/jet/internal/jet"

// UNION effectively appends the result of sub-queries(select statements) into single query.
// It eliminates duplicate rows from its result.
func UNION(lhs, rhs jet.StatementWithProjections, selects ...jet.StatementWithProjections) setStatement {
	return newSetStatementImpl(union, false, toSelectList(lhs, rhs, selects...))
}

// UNION_ALL effectively appends the result of sub-queries(select statements) into single query.
// It does not eliminates duplicate rows from its result.
func UNION_ALL(lhs, rhs jet.StatementWithProjections, selects ...jet.StatementWithProjections) setStatement {
	return newSetStatementImpl(union, true, toSelectList(lhs, rhs, selects...))
}

type setStatement interface {
	setOperators

	ORDER_BY(orderByClauses ...jet.OrderByClause) setStatement

	LIMIT(limit int64) setStatement
	OFFSET(offset int64) setStatement

	AsTable(alias string) SelectTable
}

type setOperators interface {
	jet.Statement
	jet.HasProjections
	jet.Expression

	UNION(rhs SelectStatement) setStatement
	UNION_ALL(rhs SelectStatement) setStatement
}

type setOperatorsImpl struct {
	parent setOperators
}

func (s *setOperatorsImpl) UNION(rhs SelectStatement) setStatement {
	return UNION(s.parent, rhs)
}

func (s *setOperatorsImpl) UNION_ALL(rhs SelectStatement) setStatement {
	return UNION_ALL(s.parent, rhs)
}

type setStatementImpl struct {
	jet.ExpressionStatement

	setOperatorsImpl

	setOperator jet.ClauseSetStmtOperator
}

func newSetStatementImpl(operator string, all bool, selects []jet.StatementWithProjections) setStatement {
	newSetStatement := &setStatementImpl{}
	newSetStatement.ExpressionStatement = jet.NewExpressionStatementImpl(Dialect, jet.SetStatementType, newSetStatement,
		&newSetStatement.setOperator)

	newSetStatement.setOperator.Operator = operator
	newSetStatement.setOperator.All = all
	newSetStatement.setOperator.Selects = selects
	newSetStatement.setOperator.Limit.Count = -1
	newSetStatement.setOperator.Offset.Count = -1

	newSetStatement.setOperatorsImpl.parent = newSetStatement

	return newSetStatement
}

func (s *setStatementImpl) ORDER_BY(orderByClauses ...jet.OrderByClause) setStatement {
	s.setOperator.OrderBy.List = orderByClauses
	return s
}

func (s *setStatementImpl) LIMIT(limit int64) setStatement {
	s.setOperator.Limit.Count = limit
	return s
}

func (s *setStatementImpl) OFFSET(offset int64) setStatement {
	s.setOperator.Offset.Count = offset
	return s
}

func (s *setStatementImpl) AsTable(alias string) SelectTable {
	return newSelectTable(s, alias)
}

const (
	union = "UNION"
)

func toSelectList(lhs, rhs jet.StatementWithProjections, selects ...jet.StatementWithProjections) []jet.StatementWithProjections {
	return append([]jet.StatementWithProjections{lhs, rhs}, selects...)
}
