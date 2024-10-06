package jet

// RowExpression interface
type RowExpression interface {
	Expression

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
	parent RowExpression
}

func (n *rowInterfaceImpl) EQ(rhs RowExpression) BoolExpression {
	return Eq(n.parent, rhs)
}

func (n *rowInterfaceImpl) NOT_EQ(rhs RowExpression) BoolExpression {
	return NotEq(n.parent, rhs)
}

func (n *rowInterfaceImpl) IS_DISTINCT_FROM(rhs RowExpression) BoolExpression {
	return IsDistinctFrom(n.parent, rhs)
}

func (n *rowInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs RowExpression) BoolExpression {
	return IsNotDistinctFrom(n.parent, rhs)
}

func (n *rowInterfaceImpl) GT(rhs RowExpression) BoolExpression {
	return Gt(n.parent, rhs)
}

func (n *rowInterfaceImpl) GT_EQ(rhs RowExpression) BoolExpression {
	return GtEq(n.parent, rhs)
}

func (n *rowInterfaceImpl) LT(rhs RowExpression) BoolExpression {
	return Lt(n.parent, rhs)
}

func (n *rowInterfaceImpl) LT_EQ(rhs RowExpression) BoolExpression {
	return LtEq(n.parent, rhs)
}

//---------------------------------------------------//

type rowExpressionWrapper struct {
	rowInterfaceImpl
	Expression
}

// RowExp serves as a wrapper for an arbitrary expression, treating it as a row expression.
// This enables the Go compiler to interpret any expression as a row expression
// Note: This does not modify the generated SQL builder output by adding a SQL CAST operation.
func RowExp(expression Expression) RowExpression {
	rowExpressionWrap := rowExpressionWrapper{Expression: expression}
	rowExpressionWrap.rowInterfaceImpl.parent = &rowExpressionWrap
	return &rowExpressionWrap
}

// ROW function is used to create a tuple value that consists of a set of expressions or column values.
func ROW(expressions ...Expression) RowExpression {
	return RowExp(NewFunc("ROW", expressions, nil))
}

// WRAP creates row expressions without ROW keyword `( expression1, expression2, ... )`.
func WRAP(expressions ...Expression) RowExpression {
	return RowExp(NewFunc("", expressions, nil))
}
