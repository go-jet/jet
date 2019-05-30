package sqlbuilder

type DateExpression interface {
	expression

	EQ(rhs DateExpression) boolExpression
	NOT_EQ(rhs DateExpression) boolExpression
	IS_DISTINCT_FROM(rhs DateExpression) boolExpression
	IS_NOT_DISTINCT_FROM(rhs DateExpression) boolExpression

	LT(rhs DateExpression) boolExpression
	LT_EQ(rhs DateExpression) boolExpression
	GT(rhs DateExpression) boolExpression
	GT_EQ(rhs DateExpression) boolExpression
}

type dateInterfaceImpl struct {
	parent DateExpression
}

func (t *dateInterfaceImpl) EQ(rhs DateExpression) boolExpression {
	return EQ(t.parent, rhs)
}

func (t *dateInterfaceImpl) NOT_EQ(rhs DateExpression) boolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *dateInterfaceImpl) IS_DISTINCT_FROM(rhs DateExpression) boolExpression {
	return IS_DISTINCT_FROM(t.parent, rhs)
}

func (t *dateInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs DateExpression) boolExpression {
	return IS_NOT_DISTINCT_FROM(t.parent, rhs)
}

func (t *dateInterfaceImpl) LT(rhs DateExpression) boolExpression {
	return LT(t.parent, rhs)
}

func (t *dateInterfaceImpl) LT_EQ(rhs DateExpression) boolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *dateInterfaceImpl) GT(rhs DateExpression) boolExpression {
	return GT(t.parent, rhs)
}

func (t *dateInterfaceImpl) GT_EQ(rhs DateExpression) boolExpression {
	return GT_EQ(t.parent, rhs)
}
