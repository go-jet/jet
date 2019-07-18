package jet

// TimezExpression interface 'time with time zone'
type TimezExpression interface {
	Expression

	//EQ
	EQ(rhs TimezExpression) BoolExpression
	//NOT_EQ
	NOT_EQ(rhs TimezExpression) BoolExpression
	//IS_DISTINCT_FROM
	IS_DISTINCT_FROM(rhs TimezExpression) BoolExpression
	//IS_NOT_DISTINCT_FROM
	IS_NOT_DISTINCT_FROM(rhs TimezExpression) BoolExpression

	//LT
	LT(rhs TimezExpression) BoolExpression
	//LT_EQ
	LT_EQ(rhs TimezExpression) BoolExpression
	//GT
	GT(rhs TimezExpression) BoolExpression
	//GT_EQ
	GT_EQ(rhs TimezExpression) BoolExpression
}

type timezInterfaceImpl struct {
	parent TimezExpression
}

func (t *timezInterfaceImpl) EQ(rhs TimezExpression) BoolExpression {
	return eq(t.parent, rhs)
}

func (t *timezInterfaceImpl) NOT_EQ(rhs TimezExpression) BoolExpression {
	return notEq(t.parent, rhs)
}

func (t *timezInterfaceImpl) IS_DISTINCT_FROM(rhs TimezExpression) BoolExpression {
	return isDistinctFrom(t.parent, rhs)
}

func (t *timezInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimezExpression) BoolExpression {
	return isNotDistinctFrom(t.parent, rhs)
}

func (t *timezInterfaceImpl) LT(rhs TimezExpression) BoolExpression {
	return lt(t.parent, rhs)
}

func (t *timezInterfaceImpl) LT_EQ(rhs TimezExpression) BoolExpression {
	return ltEq(t.parent, rhs)
}

func (t *timezInterfaceImpl) GT(rhs TimezExpression) BoolExpression {
	return gt(t.parent, rhs)
}

func (t *timezInterfaceImpl) GT_EQ(rhs TimezExpression) BoolExpression {
	return gtEq(t.parent, rhs)
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

// TimezExp is time with time zone expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as time with time zone expression.
// Does not add sql cast to generated sql builder output.
func TimezExp(expression Expression) TimezExpression {
	return newTimezExpressionWrap(expression)
}
