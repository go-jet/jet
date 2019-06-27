package jet

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
}

type timestampzInterfaceImpl struct {
	parent TimestampzExpression
}

func (t *timestampzInterfaceImpl) EQ(rhs TimestampzExpression) BoolExpression {
	return eq(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) NOT_EQ(rhs TimestampzExpression) BoolExpression {
	return notEq(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) IS_DISTINCT_FROM(rhs TimestampzExpression) BoolExpression {
	return isDistinctFrom(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimestampzExpression) BoolExpression {
	return isNotDistinctFrom(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) LT(rhs TimestampzExpression) BoolExpression {
	return lt(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) LT_EQ(rhs TimestampzExpression) BoolExpression {
	return ltEq(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) GT(rhs TimestampzExpression) BoolExpression {
	return gt(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) GT_EQ(rhs TimestampzExpression) BoolExpression {
	return gtEq(t.parent, rhs)
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

func TimestampzExp(expression Expression) TimestampExpression {
	return newTimestampExpressionWrap(expression)
}
