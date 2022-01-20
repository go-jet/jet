package jet

// TimeExpression interface
type TimeExpression interface {
	Expression

	EQ(rhs TimeExpression) BoolExpression
	NOT_EQ(rhs TimeExpression) BoolExpression
	IS_DISTINCT_FROM(rhs TimeExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs TimeExpression) BoolExpression

	LT(rhs TimeExpression) BoolExpression
	LT_EQ(rhs TimeExpression) BoolExpression
	GT(rhs TimeExpression) BoolExpression
	GT_EQ(rhs TimeExpression) BoolExpression
	BETWEEN(min, max TimeExpression) BoolExpression
	NOT_BETWEEN(min, max TimeExpression) BoolExpression

	ADD(rhs Interval) TimeExpression
	SUB(rhs Interval) TimeExpression
}

type timeInterfaceImpl struct {
	parent TimeExpression
}

func (t *timeInterfaceImpl) EQ(rhs TimeExpression) BoolExpression {
	return Eq(t.parent, rhs)
}

func (t *timeInterfaceImpl) NOT_EQ(rhs TimeExpression) BoolExpression {
	return NotEq(t.parent, rhs)
}

func (t *timeInterfaceImpl) IS_DISTINCT_FROM(rhs TimeExpression) BoolExpression {
	return IsDistinctFrom(t.parent, rhs)
}

func (t *timeInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimeExpression) BoolExpression {
	return IsNotDistinctFrom(t.parent, rhs)
}

func (t *timeInterfaceImpl) LT(rhs TimeExpression) BoolExpression {
	return Lt(t.parent, rhs)
}

func (t *timeInterfaceImpl) LT_EQ(rhs TimeExpression) BoolExpression {
	return LtEq(t.parent, rhs)
}

func (t *timeInterfaceImpl) GT(rhs TimeExpression) BoolExpression {
	return Gt(t.parent, rhs)
}

func (t *timeInterfaceImpl) GT_EQ(rhs TimeExpression) BoolExpression {
	return GtEq(t.parent, rhs)
}

func (t *timeInterfaceImpl) BETWEEN(min, max TimeExpression) BoolExpression {
	return NewBetweenOperatorExpression(t.parent, min, max, false)
}

func (t *timeInterfaceImpl) NOT_BETWEEN(min, max TimeExpression) BoolExpression {
	return NewBetweenOperatorExpression(t.parent, min, max, true)
}

func (t *timeInterfaceImpl) ADD(rhs Interval) TimeExpression {
	return TimeExp(Add(t.parent, rhs))
}

func (t *timeInterfaceImpl) SUB(rhs Interval) TimeExpression {
	return TimeExp(Sub(t.parent, rhs))
}

//---------------------------------------------------//

type timeExpressionWrapper struct {
	timeInterfaceImpl
	Expression
}

func newTimeExpressionWrap(expression Expression) TimeExpression {
	timeExpressionWrap := timeExpressionWrapper{Expression: expression}
	timeExpressionWrap.timeInterfaceImpl.parent = &timeExpressionWrap
	return &timeExpressionWrap
}

// TimeExp is time expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as time expression.
// Does not add sql cast to generated sql builder output.
func TimeExp(expression Expression) TimeExpression {
	return newTimeExpressionWrap(expression)
}
