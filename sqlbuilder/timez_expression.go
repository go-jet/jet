package sqlbuilder

type timezExpression interface {
	expression

	EQ(rhs timezExpression) BoolExpression
	NOT_EQ(rhs timezExpression) BoolExpression
	IS_DISTINCT_FROM(rhs timezExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs timezExpression) BoolExpression

	LT(rhs timezExpression) BoolExpression
	LT_EQ(rhs timezExpression) BoolExpression
	GT(rhs timezExpression) BoolExpression
	GT_EQ(rhs timezExpression) BoolExpression
}

type timezInterfaceImpl struct {
	parent timezExpression
}

func (t *timezInterfaceImpl) EQ(rhs timezExpression) BoolExpression {
	return EQ(t.parent, rhs)
}

func (t *timezInterfaceImpl) NOT_EQ(rhs timezExpression) BoolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *timezInterfaceImpl) IS_DISTINCT_FROM(rhs timezExpression) BoolExpression {
	return IS_DISTINCT_FROM(t.parent, rhs)
}

func (t *timezInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs timezExpression) BoolExpression {
	return IS_NOT_DISTINCT_FROM(t.parent, rhs)
}

func (t *timezInterfaceImpl) LT(rhs timezExpression) BoolExpression {
	return LT(t.parent, rhs)
}

func (t *timezInterfaceImpl) LT_EQ(rhs timezExpression) BoolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *timezInterfaceImpl) GT(rhs timezExpression) BoolExpression {
	return GT(t.parent, rhs)
}

func (t *timezInterfaceImpl) GT_EQ(rhs timezExpression) BoolExpression {
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
