package jet

// TimezExpression interface for 'time with time zone' types
type TimezExpression interface {
	Expression

	EQ(rhs TimezExpression) BoolExpression
	NOT_EQ(rhs TimezExpression) BoolExpression
	IS_DISTINCT_FROM(rhs TimezExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs TimezExpression) BoolExpression

	LT(rhs TimezExpression) BoolExpression
	LT_EQ(rhs TimezExpression) BoolExpression
	GT(rhs TimezExpression) BoolExpression
	GT_EQ(rhs TimezExpression) BoolExpression
	BETWEEN(min, max TimezExpression) BoolExpression
	NOT_BETWEEN(min, max TimezExpression) BoolExpression

	ADD(rhs Interval) TimezExpression
	SUB(rhs Interval) TimezExpression
}

type timezInterfaceImpl struct {
	root TimezExpression
}

func (t *timezInterfaceImpl) EQ(rhs TimezExpression) BoolExpression {
	return Eq(t.root, rhs)
}

func (t *timezInterfaceImpl) NOT_EQ(rhs TimezExpression) BoolExpression {
	return NotEq(t.root, rhs)
}

func (t *timezInterfaceImpl) IS_DISTINCT_FROM(rhs TimezExpression) BoolExpression {
	return IsDistinctFrom(t.root, rhs)
}

func (t *timezInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimezExpression) BoolExpression {
	return IsNotDistinctFrom(t.root, rhs)
}

func (t *timezInterfaceImpl) LT(rhs TimezExpression) BoolExpression {
	return Lt(t.root, rhs)
}

func (t *timezInterfaceImpl) LT_EQ(rhs TimezExpression) BoolExpression {
	return LtEq(t.root, rhs)
}

func (t *timezInterfaceImpl) GT(rhs TimezExpression) BoolExpression {
	return Gt(t.root, rhs)
}

func (t *timezInterfaceImpl) GT_EQ(rhs TimezExpression) BoolExpression {
	return GtEq(t.root, rhs)
}

func (t *timezInterfaceImpl) BETWEEN(min, max TimezExpression) BoolExpression {
	return NewBetweenOperatorExpression(t.root, min, max, false)
}

func (t *timezInterfaceImpl) NOT_BETWEEN(min, max TimezExpression) BoolExpression {
	return NewBetweenOperatorExpression(t.root, min, max, true)
}

func (t *timezInterfaceImpl) ADD(rhs Interval) TimezExpression {
	return TimezExp(Add(t.root, rhs))
}

func (t *timezInterfaceImpl) SUB(rhs Interval) TimezExpression {
	return TimezExp(Sub(t.root, rhs))
}

//---------------------------------------------------//

type timezExpressionWrapper struct {
	Expression
	timezInterfaceImpl
}

func newTimezExpressionWrap(expression Expression) TimezExpression {
	timezExpressionWrap := &timezExpressionWrapper{Expression: expression}
	timezExpressionWrap.timezInterfaceImpl.root = timezExpressionWrap
	expression.setRoot(timezExpressionWrap)
	return timezExpressionWrap
}

// TimezExp is time with time zone expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as time with time zone expression.
// Does not add sql cast to generated sql builder output.
func TimezExp(expression Expression) TimezExpression {
	return newTimezExpressionWrap(expression)
}
