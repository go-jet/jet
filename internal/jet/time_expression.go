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
	root TimeExpression
}

func (t *timeInterfaceImpl) EQ(rhs TimeExpression) BoolExpression {
	return Eq(t.root, rhs)
}

func (t *timeInterfaceImpl) NOT_EQ(rhs TimeExpression) BoolExpression {
	return NotEq(t.root, rhs)
}

func (t *timeInterfaceImpl) IS_DISTINCT_FROM(rhs TimeExpression) BoolExpression {
	return IsDistinctFrom(t.root, rhs)
}

func (t *timeInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimeExpression) BoolExpression {
	return IsNotDistinctFrom(t.root, rhs)
}

func (t *timeInterfaceImpl) LT(rhs TimeExpression) BoolExpression {
	return Lt(t.root, rhs)
}

func (t *timeInterfaceImpl) LT_EQ(rhs TimeExpression) BoolExpression {
	return LtEq(t.root, rhs)
}

func (t *timeInterfaceImpl) GT(rhs TimeExpression) BoolExpression {
	return Gt(t.root, rhs)
}

func (t *timeInterfaceImpl) GT_EQ(rhs TimeExpression) BoolExpression {
	return GtEq(t.root, rhs)
}

func (t *timeInterfaceImpl) BETWEEN(min, max TimeExpression) BoolExpression {
	return NewBetweenOperatorExpression(t.root, min, max, false)
}

func (t *timeInterfaceImpl) NOT_BETWEEN(min, max TimeExpression) BoolExpression {
	return NewBetweenOperatorExpression(t.root, min, max, true)
}

func (t *timeInterfaceImpl) ADD(rhs Interval) TimeExpression {
	return TimeExp(Add(t.root, rhs))
}

func (t *timeInterfaceImpl) SUB(rhs Interval) TimeExpression {
	return TimeExp(Sub(t.root, rhs))
}

//---------------------------------------------------//

type timeExpressionWrapper struct {
	Expression
	timeInterfaceImpl
}

func newTimeExpressionWrap(expression Expression) TimeExpression {
	timeExpressionWrap := &timeExpressionWrapper{Expression: expression}
	timeExpressionWrap.timeInterfaceImpl.root = timeExpressionWrap
	expression.setRoot(timeExpressionWrap)
	return timeExpressionWrap
}

// TimeExp is time expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as time expression.
// Does not add sql cast to generated sql builder output.
func TimeExp(expression Expression) TimeExpression {
	return newTimeExpressionWrap(expression)
}
