package jet

// DateExpression is interface for all SQL date expressions.
type DateExpression interface {
	Expression

	EQ(rhs DateExpression) BoolExpression
	NOT_EQ(rhs DateExpression) BoolExpression
	IS_DISTINCT_FROM(rhs DateExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs DateExpression) BoolExpression

	LT(rhs DateExpression) BoolExpression
	LT_EQ(rhs DateExpression) BoolExpression
	GT(rhs DateExpression) BoolExpression
	GT_EQ(rhs DateExpression) BoolExpression
}

type dateInterfaceImpl struct {
	parent DateExpression
}

func (t *dateInterfaceImpl) EQ(rhs DateExpression) BoolExpression {
	return eq(t.parent, rhs)
}

func (t *dateInterfaceImpl) NOT_EQ(rhs DateExpression) BoolExpression {
	return notEq(t.parent, rhs)
}

func (t *dateInterfaceImpl) IS_DISTINCT_FROM(rhs DateExpression) BoolExpression {
	return isDistinctFrom(t.parent, rhs)
}

func (t *dateInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs DateExpression) BoolExpression {
	return isNotDistinctFrom(t.parent, rhs)
}

func (t *dateInterfaceImpl) LT(rhs DateExpression) BoolExpression {
	return lt(t.parent, rhs)
}

func (t *dateInterfaceImpl) LT_EQ(rhs DateExpression) BoolExpression {
	return ltEq(t.parent, rhs)
}

func (t *dateInterfaceImpl) GT(rhs DateExpression) BoolExpression {
	return gt(t.parent, rhs)
}

func (t *dateInterfaceImpl) GT_EQ(rhs DateExpression) BoolExpression {
	return gtEq(t.parent, rhs)
}

//---------------------------------------------------//

type dateExpressionWrapper struct {
	dateInterfaceImpl
	Expression
}

func newDateExpressionWrap(expression Expression) DateExpression {
	dateExpressionWrap := dateExpressionWrapper{Expression: expression}
	dateExpressionWrap.dateInterfaceImpl.parent = &dateExpressionWrap
	return &dateExpressionWrap
}

// DateExp is date expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as date expression.
// Does not add sql cast to generated sql builder output.
func DateExp(expression Expression) DateExpression {
	return newDateExpressionWrap(expression)
}
