package jet

// TimestampzExpression interface
type TimestampzExpression interface {
	Expression

	EQ(rhs TimestampzExpression) BoolExpression
	NOT_EQ(rhs TimestampzExpression) BoolExpression
	IS_DISTINCT_FROM(rhs TimestampzExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs TimestampzExpression) BoolExpression

	LT(rhs TimestampzExpression) BoolExpression
	LT_EQ(rhs TimestampzExpression) BoolExpression
	GT(rhs TimestampzExpression) BoolExpression
	GT_EQ(rhs TimestampzExpression) BoolExpression
	BETWEEN(min, max TimestampzExpression) BoolExpression
	NOT_BETWEEN(min, max TimestampzExpression) BoolExpression

	ADD(rhs Interval) TimestampzExpression
	SUB(rhs Interval) TimestampzExpression
}

type timestampzInterfaceImpl struct {
	parent TimestampzExpression
}

func (t *timestampzInterfaceImpl) EQ(rhs TimestampzExpression) BoolExpression {
	return Eq(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) NOT_EQ(rhs TimestampzExpression) BoolExpression {
	return NotEq(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) IS_DISTINCT_FROM(rhs TimestampzExpression) BoolExpression {
	return IsDistinctFrom(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimestampzExpression) BoolExpression {
	return IsNotDistinctFrom(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) LT(rhs TimestampzExpression) BoolExpression {
	return Lt(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) LT_EQ(rhs TimestampzExpression) BoolExpression {
	return LtEq(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) GT(rhs TimestampzExpression) BoolExpression {
	return Gt(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) GT_EQ(rhs TimestampzExpression) BoolExpression {
	return GtEq(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) BETWEEN(min, max TimestampzExpression) BoolExpression {
	return NewBetweenOperatorExpression(t.parent, min, max, false)
}

func (t *timestampzInterfaceImpl) NOT_BETWEEN(min, max TimestampzExpression) BoolExpression {
	return NewBetweenOperatorExpression(t.parent, min, max, true)
}

func (t *timestampzInterfaceImpl) ADD(rhs Interval) TimestampzExpression {
	return TimestampzExp(Add(t.parent, rhs))
}

func (t *timestampzInterfaceImpl) SUB(rhs Interval) TimestampzExpression {
	return TimestampzExp(Sub(t.parent, rhs))
}

//-------------------------------------------------

type timestampzExpressionWrapper struct {
	timestampzInterfaceImpl
	Expression
}

func newTimestampzExpressionWrap(expression Expression) TimestampzExpression {
	timestampzExpressionWrap := timestampzExpressionWrapper{Expression: expression}
	timestampzExpressionWrap.timestampzInterfaceImpl.parent = &timestampzExpressionWrap
	return &timestampzExpressionWrap
}

// TimestampzExp is timestamp with time zone expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as timestamp with time zone expression.
// Does not add sql cast to generated sql builder output.
func TimestampzExp(expression Expression) TimestampzExpression {
	return newTimestampzExpressionWrap(expression)
}
