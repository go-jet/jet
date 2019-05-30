package sqlbuilder

type TimestampzExpression interface {
	expression

	EQ(rhs TimestampzExpression) boolExpression
	NOT_EQ(rhs TimestampzExpression) boolExpression
	IS_DISTINCT_FROM(rhs TimestampzExpression) boolExpression
	IS_NOT_DISTINCT_FROM(rhs TimestampzExpression) boolExpression

	LT(rhs TimestampzExpression) boolExpression
	LT_EQ(rhs TimestampzExpression) boolExpression
	GT(rhs TimestampzExpression) boolExpression
	GT_EQ(rhs TimestampzExpression) boolExpression
}

type timestampzInterfaceImpl struct {
	parent TimestampzExpression
}

func (t *timestampzInterfaceImpl) EQ(rhs TimestampzExpression) boolExpression {
	return EQ(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) NOT_EQ(rhs TimestampzExpression) boolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) IS_DISTINCT_FROM(rhs TimestampzExpression) boolExpression {
	return IS_DISTINCT_FROM(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimestampzExpression) boolExpression {
	return IS_NOT_DISTINCT_FROM(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) LT(rhs TimestampzExpression) boolExpression {
	return LT(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) LT_EQ(rhs TimestampzExpression) boolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) GT(rhs TimestampzExpression) boolExpression {
	return GT(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) GT_EQ(rhs TimestampzExpression) boolExpression {
	return GT_EQ(t.parent, rhs)
}
