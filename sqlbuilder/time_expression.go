package sqlbuilder

type timeExpression interface {
	expression

	EQ(rhs timeExpression) BoolExpression
	NOT_EQ(rhs timeExpression) BoolExpression
	IS_DISTINCT_FROM(rhs timeExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs timeExpression) BoolExpression

	LT(rhs timeExpression) BoolExpression
	LT_EQ(rhs timeExpression) BoolExpression
	GT(rhs timeExpression) BoolExpression
	GT_EQ(rhs timeExpression) BoolExpression
}

type timeInterfaceImpl struct {
	parent timeExpression
}

func (t *timeInterfaceImpl) EQ(rhs timeExpression) BoolExpression {
	return EQ(t.parent, rhs)
}

func (t *timeInterfaceImpl) NOT_EQ(rhs timeExpression) BoolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *timeInterfaceImpl) IS_DISTINCT_FROM(rhs timeExpression) BoolExpression {
	return IS_DISTINCT_FROM(t.parent, rhs)
}

func (t *timeInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs timeExpression) BoolExpression {
	return IS_NOT_DISTINCT_FROM(t.parent, rhs)
}

func (t *timeInterfaceImpl) LT(rhs timeExpression) BoolExpression {
	return LT(t.parent, rhs)
}

func (t *timeInterfaceImpl) LT_EQ(rhs timeExpression) BoolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *timeInterfaceImpl) GT(rhs timeExpression) BoolExpression {
	return GT(t.parent, rhs)
}

func (t *timeInterfaceImpl) GT_EQ(rhs timeExpression) BoolExpression {
	return GT_EQ(t.parent, rhs)
}

//---------------------------------------------------//
type prefixTimeExpression struct {
	expressionInterfaceImpl
	timeInterfaceImpl

	prefixOpExpression
}

func newPrefixTimeExpression(operator string, expression expression) timeExpression {
	timeExpr := prefixTimeExpression{}
	timeExpr.prefixOpExpression = newPrefixExpression(expression, operator)

	timeExpr.expressionInterfaceImpl.parent = &timeExpr
	timeExpr.timeInterfaceImpl.parent = &timeExpr

	return &timeExpr
}

func INTERVAL(interval string) expression {
	return newPrefixTimeExpression("INTERVAL", Literal(interval))
}
