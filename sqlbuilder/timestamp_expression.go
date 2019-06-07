package sqlbuilder

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
}

type timestampInterfaceImpl struct {
	parent TimestampExpression
}

func (t *timestampInterfaceImpl) EQ(rhs TimestampExpression) BoolExpression {
	return EQ(t.parent, rhs)
}

func (t *timestampInterfaceImpl) NOT_EQ(rhs TimestampExpression) BoolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *timestampInterfaceImpl) IS_DISTINCT_FROM(rhs TimestampExpression) BoolExpression {
	return IS_DISTINCT_FROM(t.parent, rhs)
}

func (t *timestampInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimestampExpression) BoolExpression {
	return IS_NOT_DISTINCT_FROM(t.parent, rhs)
}

func (t *timestampInterfaceImpl) LT(rhs TimestampExpression) BoolExpression {
	return LT(t.parent, rhs)
}

func (t *timestampInterfaceImpl) LT_EQ(rhs TimestampExpression) BoolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *timestampInterfaceImpl) GT(rhs TimestampExpression) BoolExpression {
	return GT(t.parent, rhs)
}

func (t *timestampInterfaceImpl) GT_EQ(rhs TimestampExpression) BoolExpression {
	return GT_EQ(t.parent, rhs)
}

//-------------------------------------------------

type timestampExpressionWrapper struct {
	timestampInterfaceImpl
	Expression
}

func newTimestampExpressionWrap(expression Expression) TimestampExpression {
	timestampExpressionWrap := timestampExpressionWrapper{Expression: expression}
	timestampExpressionWrap.timestampInterfaceImpl.parent = &timestampExpressionWrap
	return &timestampExpressionWrap
}

func TimestampExp(expression Expression) TimestampExpression {
	return newTimestampExpressionWrap(expression)
}
