package sqlbuilder

type TimestampzExpression interface {
	expression

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
	return EQ(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) NOT_EQ(rhs TimestampzExpression) BoolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) IS_DISTINCT_FROM(rhs TimestampzExpression) BoolExpression {
	return IS_DISTINCT_FROM(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimestampzExpression) BoolExpression {
	return IS_NOT_DISTINCT_FROM(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) LT(rhs TimestampzExpression) BoolExpression {
	return LT(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) LT_EQ(rhs TimestampzExpression) BoolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) GT(rhs TimestampzExpression) BoolExpression {
	return GT(t.parent, rhs)
}

func (t *timestampzInterfaceImpl) GT_EQ(rhs TimestampzExpression) BoolExpression {
	return GT_EQ(t.parent, rhs)
}
