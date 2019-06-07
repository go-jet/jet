package sqlbuilder

type TimezExpression interface {
	Expression

	EQ(rhs TimezExpression) BoolExpression
	NOT_EQ(rhs TimezExpression) BoolExpression
	IS_DISTINCT_FROM(rhs TimezExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs TimezExpression) BoolExpression

	LT(rhs TimezExpression) BoolExpression
	LT_EQ(rhs TimezExpression) BoolExpression
	GT(rhs TimezExpression) BoolExpression
	GT_EQ(rhs TimezExpression) BoolExpression
}

type timezInterfaceImpl struct {
	parent TimezExpression
}

func (t *timezInterfaceImpl) EQ(rhs TimezExpression) BoolExpression {
	return EQ(t.parent, rhs)
}

func (t *timezInterfaceImpl) NOT_EQ(rhs TimezExpression) BoolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *timezInterfaceImpl) IS_DISTINCT_FROM(rhs TimezExpression) BoolExpression {
	return IS_DISTINCT_FROM(t.parent, rhs)
}

func (t *timezInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimezExpression) BoolExpression {
	return IS_NOT_DISTINCT_FROM(t.parent, rhs)
}

func (t *timezInterfaceImpl) LT(rhs TimezExpression) BoolExpression {
	return LT(t.parent, rhs)
}

func (t *timezInterfaceImpl) LT_EQ(rhs TimezExpression) BoolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *timezInterfaceImpl) GT(rhs TimezExpression) BoolExpression {
	return GT(t.parent, rhs)
}

func (t *timezInterfaceImpl) GT_EQ(rhs TimezExpression) BoolExpression {
	return GT_EQ(t.parent, rhs)
}

//---------------------------------------------------//
type prefixTimezExpression struct {
	expressionInterfaceImpl
	timezInterfaceImpl

	prefixOpExpression
}

func newPrefixTimezExpression(operator string, expression Expression) TimezExpression {
	timeExpr := prefixTimezExpression{}
	timeExpr.prefixOpExpression = newPrefixExpression(expression, operator)

	timeExpr.expressionInterfaceImpl.parent = &timeExpr
	timeExpr.timezInterfaceImpl.parent = &timeExpr

	return &timeExpr
}

//---------------------------------------------------//

type timezExpressionWrapper struct {
	timezInterfaceImpl
	Expression
}

func newTimezExpressionWrap(expression Expression) TimezExpression {
	timezExpressionWrap := timezExpressionWrapper{Expression: expression}
	timezExpressionWrap.timezInterfaceImpl.parent = &timezExpressionWrap
	return &timezExpressionWrap
}

func TimezExp(expression Expression) TimeExpression {
	return newTimeExpressionWrap(expression)
}
