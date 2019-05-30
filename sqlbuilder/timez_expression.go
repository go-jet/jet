package sqlbuilder

type timezExpression interface {
	expression

	EQ(rhs timezExpression) boolExpression
	NOT_EQ(rhs timezExpression) boolExpression
	IS_DISTINCT_FROM(rhs timezExpression) boolExpression
	IS_NOT_DISTINCT_FROM(rhs timezExpression) boolExpression

	LT(rhs timezExpression) boolExpression
	LT_EQ(rhs timezExpression) boolExpression
	GT(rhs timezExpression) boolExpression
	GT_EQ(rhs timezExpression) boolExpression
}

type timezInterfaceImpl struct {
	parent timezExpression
}

func (t *timezInterfaceImpl) EQ(rhs timezExpression) boolExpression {
	return EQ(t.parent, rhs)
}

func (t *timezInterfaceImpl) NOT_EQ(rhs timezExpression) boolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *timezInterfaceImpl) IS_DISTINCT_FROM(rhs timezExpression) boolExpression {
	return IS_DISTINCT_FROM(t.parent, rhs)
}

func (t *timezInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs timezExpression) boolExpression {
	return IS_NOT_DISTINCT_FROM(t.parent, rhs)
}

func (t *timezInterfaceImpl) LT(rhs timezExpression) boolExpression {
	return LT(t.parent, rhs)
}

func (t *timezInterfaceImpl) LT_EQ(rhs timezExpression) boolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *timezInterfaceImpl) GT(rhs timezExpression) boolExpression {
	return GT(t.parent, rhs)
}

func (t *timezInterfaceImpl) GT_EQ(rhs timezExpression) boolExpression {
	return GT_EQ(t.parent, rhs)
}

//---------------------------------------------------//
type prefixTimezExpression struct {
	expressionInterfaceImpl
	timezInterfaceImpl

	prefixOpExpression
}

func newPrefixTimezExpression(operator string, expression expression) timezExpression {
	timeExpr := prefixTimezExpression{}
	timeExpr.prefixOpExpression = newPrefixExpression(expression, operator)

	timeExpr.expressionInterfaceImpl.parent = &timeExpr
	timeExpr.timezInterfaceImpl.parent = &timeExpr

	return &timeExpr
}
