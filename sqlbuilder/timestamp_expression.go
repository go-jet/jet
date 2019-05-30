package sqlbuilder

type TimestampExpression interface {
	expression

	EQ(rhs TimestampExpression) boolExpression
	NOT_EQ(rhs TimestampExpression) boolExpression
	IS_DISTINCT_FROM(rhs TimestampExpression) boolExpression
	IS_NOT_DISTINCT_FROM(rhs TimestampExpression) boolExpression

	LT(rhs TimestampExpression) boolExpression
	LT_EQ(rhs TimestampExpression) boolExpression
	GT(rhs TimestampExpression) boolExpression
	GT_EQ(rhs TimestampExpression) boolExpression
}

type timestampInterfaceImpl struct {
	parent TimestampExpression
}

func (t *timestampInterfaceImpl) EQ(rhs TimestampExpression) boolExpression {
	return EQ(t.parent, rhs)
}

func (t *timestampInterfaceImpl) NOT_EQ(rhs TimestampExpression) boolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *timestampInterfaceImpl) IS_DISTINCT_FROM(rhs TimestampExpression) boolExpression {
	return IS_DISTINCT_FROM(t.parent, rhs)
}

func (t *timestampInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimestampExpression) boolExpression {
	return IS_NOT_DISTINCT_FROM(t.parent, rhs)
}

func (t *timestampInterfaceImpl) LT(rhs TimestampExpression) boolExpression {
	return LT(t.parent, rhs)
}

func (t *timestampInterfaceImpl) LT_EQ(rhs TimestampExpression) boolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *timestampInterfaceImpl) GT(rhs TimestampExpression) boolExpression {
	return GT(t.parent, rhs)
}

func (t *timestampInterfaceImpl) GT_EQ(rhs TimestampExpression) boolExpression {
	return GT_EQ(t.parent, rhs)
}
