package sqlbuilder

type DateExpression interface {
	Expression

	EQ(rhs DateExpression) BoolExpression
	NOT_EQ(rhs DateExpression) BoolExpression
	IS_DISTINCT_FROM(rhs DateExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs DateExpression) BoolExpression

	LT(rhs DateExpression) BoolExpression
	LT_EQ(rhs DateExpression) BoolExpression
	GT(rhs DateExpression) BoolExpression
	GT_EQ(rhs DateExpression) BoolExpression
}

type dateInterfaceImpl struct {
	parent DateExpression
}

func (t *dateInterfaceImpl) EQ(rhs DateExpression) BoolExpression {
	return EQ(t.parent, rhs)
}

func (t *dateInterfaceImpl) NOT_EQ(rhs DateExpression) BoolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *dateInterfaceImpl) IS_DISTINCT_FROM(rhs DateExpression) BoolExpression {
	return IS_DISTINCT_FROM(t.parent, rhs)
}

func (t *dateInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs DateExpression) BoolExpression {
	return IS_NOT_DISTINCT_FROM(t.parent, rhs)
}

func (t *dateInterfaceImpl) LT(rhs DateExpression) BoolExpression {
	return LT(t.parent, rhs)
}

func (t *dateInterfaceImpl) LT_EQ(rhs DateExpression) BoolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *dateInterfaceImpl) GT(rhs DateExpression) BoolExpression {
	return GT(t.parent, rhs)
}

func (t *dateInterfaceImpl) GT_EQ(rhs DateExpression) BoolExpression {
	return GT_EQ(t.parent, rhs)
}
