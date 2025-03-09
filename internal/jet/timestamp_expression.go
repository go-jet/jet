package jet

// TimestampExpression interface
type TimestampExpression interface {
	Expression

	EQ(rhs TimestampExpression) BoolExpression
	NOT_EQ(rhs TimestampExpression) BoolExpression
	IS_DISTINCT_FROM(rhs TimestampExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs TimestampExpression) BoolExpression

	LT(rhs TimestampExpression) BoolExpression
	LT_EQ(rhs TimestampExpression) BoolExpression
	GT(rhs TimestampExpression) BoolExpression
	GT_EQ(rhs TimestampExpression) BoolExpression
	BETWEEN(min, max TimestampExpression) BoolExpression
	NOT_BETWEEN(min, max TimestampExpression) BoolExpression

	ADD(rhs Interval) TimestampExpression
	SUB(rhs Interval) TimestampExpression
}

type timestampInterfaceImpl struct {
	root TimestampExpression
}

func (t *timestampInterfaceImpl) EQ(rhs TimestampExpression) BoolExpression {
	return Eq(t.root, rhs)
}

func (t *timestampInterfaceImpl) NOT_EQ(rhs TimestampExpression) BoolExpression {
	return NotEq(t.root, rhs)
}

func (t *timestampInterfaceImpl) IS_DISTINCT_FROM(rhs TimestampExpression) BoolExpression {
	return IsDistinctFrom(t.root, rhs)
}

func (t *timestampInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimestampExpression) BoolExpression {
	return IsNotDistinctFrom(t.root, rhs)
}

func (t *timestampInterfaceImpl) LT(rhs TimestampExpression) BoolExpression {
	return Lt(t.root, rhs)
}

func (t *timestampInterfaceImpl) LT_EQ(rhs TimestampExpression) BoolExpression {
	return LtEq(t.root, rhs)
}

func (t *timestampInterfaceImpl) GT(rhs TimestampExpression) BoolExpression {
	return Gt(t.root, rhs)
}

func (t *timestampInterfaceImpl) GT_EQ(rhs TimestampExpression) BoolExpression {
	return GtEq(t.root, rhs)
}

func (t *timestampInterfaceImpl) BETWEEN(min, max TimestampExpression) BoolExpression {
	return NewBetweenOperatorExpression(t.root, min, max, false)
}

func (t *timestampInterfaceImpl) NOT_BETWEEN(min, max TimestampExpression) BoolExpression {
	return NewBetweenOperatorExpression(t.root, min, max, true)
}

func (t *timestampInterfaceImpl) ADD(rhs Interval) TimestampExpression {
	return TimestampExp(Add(t.root, rhs))
}

func (t *timestampInterfaceImpl) SUB(rhs Interval) TimestampExpression {
	return TimestampExp(Sub(t.root, rhs))
}

//-------------------------------------------------

type timestampExpressionWrapper struct {
	timestampInterfaceImpl
	Expression
}

func newTimestampExpressionWrap(expression Expression) TimestampExpression {
	timestampExpressionWrap := &timestampExpressionWrapper{Expression: expression}
	timestampExpressionWrap.timestampInterfaceImpl.root = timestampExpressionWrap
	expression.setRoot(timestampExpressionWrap)
	return timestampExpressionWrap
}

// TimestampExp is timestamp expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as timestamp expression.
// Does not add sql cast to generated sql builder output.
func TimestampExp(expression Expression) TimestampExpression {
	return newTimestampExpressionWrap(expression)
}
