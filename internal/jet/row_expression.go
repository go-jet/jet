package jet

// RowExpression interface
type RowExpression interface {
	Expression
	HasProjections

	EQ(rhs RowExpression) BoolExpression
	NOT_EQ(rhs RowExpression) BoolExpression
	IS_DISTINCT_FROM(rhs RowExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs RowExpression) BoolExpression

	LT(rhs RowExpression) BoolExpression
	LT_EQ(rhs RowExpression) BoolExpression
	GT(rhs RowExpression) BoolExpression
	GT_EQ(rhs RowExpression) BoolExpression
}

type rowInterfaceImpl struct {
	root        Expression
	dialect     Dialect
	expressions []Expression
}

func (n *rowInterfaceImpl) EQ(rhs RowExpression) BoolExpression {
	return Eq(n.root, rhs)
}

func (n *rowInterfaceImpl) NOT_EQ(rhs RowExpression) BoolExpression {
	return NotEq(n.root, rhs)
}

func (n *rowInterfaceImpl) IS_DISTINCT_FROM(rhs RowExpression) BoolExpression {
	return IsDistinctFrom(n.root, rhs)
}

func (n *rowInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs RowExpression) BoolExpression {
	return IsNotDistinctFrom(n.root, rhs)
}

func (n *rowInterfaceImpl) GT(rhs RowExpression) BoolExpression {
	return Gt(n.root, rhs)
}

func (n *rowInterfaceImpl) GT_EQ(rhs RowExpression) BoolExpression {
	return GtEq(n.root, rhs)
}

func (n *rowInterfaceImpl) LT(rhs RowExpression) BoolExpression {
	return Lt(n.root, rhs)
}

func (n *rowInterfaceImpl) LT_EQ(rhs RowExpression) BoolExpression {
	return LtEq(n.root, rhs)
}

func (n *rowInterfaceImpl) projections() ProjectionList {
	var ret ProjectionList

	for i, expression := range n.expressions {
		ret = append(ret, newDummyColumnForExpression(expression, n.dialect.ValuesDefaultColumnName(i)))
	}

	return ret
}

// ---------------------------------------------------//
type rowExpressionWrapper struct {
	rowInterfaceImpl
	Expression
}

func newRowExpression(name string, dialect Dialect, expressions ...Expression) RowExpression {
	ret := &rowExpressionWrapper{}
	ret.rowInterfaceImpl.root = ret

	ret.Expression = NewFunc(name, expressions, ret)
	ret.dialect = dialect
	ret.expressions = expressions

	return ret
}

// ROW function is used to create a tuple value that consists of a set of expressions or column values.
func ROW(dialect Dialect, expressions ...Expression) RowExpression {
	return newRowExpression("ROW", dialect, expressions...)
}

// WRAP creates row expressions without ROW keyword `( expression1, expression2, ... )`.
func WRAP(dialect Dialect, expressions ...Expression) RowExpression {
	return newRowExpression("", dialect, expressions...)
}

// RowExp serves as a wrapper for an arbitrary expression, treating it as a row expression.
// This enables the Go compiler to interpret any expression as a row expression
// Note: This does not modify the generated SQL builder output by adding a SQL CAST operation.
func RowExp(expression Expression) RowExpression {
	rowExpressionWrap := rowExpressionWrapper{Expression: expression}
	rowExpressionWrap.rowInterfaceImpl.root = &rowExpressionWrap
	return &rowExpressionWrap
}
