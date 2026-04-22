package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

// UNION appends results of sub-queries, eliminating duplicates.
func UNION(lhs, rhs jet.SerializerStatement, selects ...jet.SerializerStatement) setStatement {
	return newSetStatementImpl("UNION", false, toSelectList(lhs, rhs, selects...))
}

// UNION_ALL appends results without eliminating duplicates.
func UNION_ALL(lhs, rhs jet.SerializerStatement, selects ...jet.SerializerStatement) setStatement {
	return newSetStatementImpl("UNION", true, toSelectList(lhs, rhs, selects...))
}

// INTERSECT returns rows in both result sets.
func INTERSECT(lhs, rhs jet.SerializerStatement, selects ...jet.SerializerStatement) setStatement {
	return newSetStatementImpl("INTERSECT", false, toSelectList(lhs, rhs, selects...))
}

// INTERSECT_ALL returns common rows without eliminating duplicates.
func INTERSECT_ALL(lhs, rhs jet.SerializerStatement, selects ...jet.SerializerStatement) setStatement {
	return newSetStatementImpl("INTERSECT", true, toSelectList(lhs, rhs, selects...))
}

// EXCEPT returns rows from first set not in second.
func EXCEPT(lhs, rhs jet.SerializerStatement, selects ...jet.SerializerStatement) setStatement {
	return newSetStatementImpl("EXCEPT", false, toSelectList(lhs, rhs, selects...))
}

// EXCEPT_ALL returns difference without eliminating duplicates.
func EXCEPT_ALL(lhs, rhs jet.SerializerStatement, selects ...jet.SerializerStatement) setStatement {
	return newSetStatementImpl("EXCEPT", true, toSelectList(lhs, rhs, selects...))
}

type setStatement interface {
	setOperators
	ORDER_BY(orderByClauses ...OrderByClause) setStatement
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
	INTERSECT(rhs SelectStatement) setStatement
	INTERSECT_ALL(rhs SelectStatement) setStatement
	EXCEPT(rhs SelectStatement) setStatement
	EXCEPT_ALL(rhs SelectStatement) setStatement
}

type setOperatorsImpl struct{ root setOperators }

func (s *setOperatorsImpl) UNION(rhs SelectStatement) setStatement         { return UNION(s.root, rhs) }
func (s *setOperatorsImpl) UNION_ALL(rhs SelectStatement) setStatement     { return UNION_ALL(s.root, rhs) }
func (s *setOperatorsImpl) INTERSECT(rhs SelectStatement) setStatement     { return INTERSECT(s.root, rhs) }
func (s *setOperatorsImpl) INTERSECT_ALL(rhs SelectStatement) setStatement { return INTERSECT_ALL(s.root, rhs) }
func (s *setOperatorsImpl) EXCEPT(rhs SelectStatement) setStatement        { return EXCEPT(s.root, rhs) }
func (s *setOperatorsImpl) EXCEPT_ALL(rhs SelectStatement) setStatement    { return EXCEPT_ALL(s.root, rhs) }

type setStatementImpl struct {
	jet.ExpressionStatement
	setOperatorsImpl
	setOperator jet.ClauseSetStmtOperator
}

func newSetStatementImpl(operator string, all bool, selects []jet.SerializerStatement) setStatement {
	ss := &setStatementImpl{}
	ss.ExpressionStatement = jet.NewExpressionStatementImpl(Dialect, jet.SetStatementType, ss, &ss.setOperator)
	ss.setOperator.Operator = operator
	ss.setOperator.All = all
	ss.setOperator.Selects = selects
	ss.setOperator.Limit.Count = -1
	ss.setOperatorsImpl.root = ss
	return ss
}

func (s *setStatementImpl) ORDER_BY(o ...OrderByClause) setStatement { s.setOperator.OrderBy.List = o; return s }
func (s *setStatementImpl) LIMIT(l int64) setStatement               { s.setOperator.Limit.Count = l; return s }
func (s *setStatementImpl) OFFSET(o int64) setStatement              { s.setOperator.Offset.Count = Int(o); return s }
func (s *setStatementImpl) AsTable(alias string) SelectTable         { return newSelectTable(s, alias, nil) }

func toSelectList(lhs, rhs jet.SerializerStatement, selects ...jet.SerializerStatement) []jet.SerializerStatement {
	return append([]jet.SerializerStatement{lhs, rhs}, selects...)
}
