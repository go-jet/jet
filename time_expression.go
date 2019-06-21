package jet

type TimeExpression interface {
	Expression

	EQ(rhs TimeExpression) BoolExpression
	NOT_EQ(rhs TimeExpression) BoolExpression
	IS_DISTINCT_FROM(rhs TimeExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs TimeExpression) BoolExpression

	LT(rhs TimeExpression) BoolExpression
	LT_EQ(rhs TimeExpression) BoolExpression
	GT(rhs TimeExpression) BoolExpression
	GT_EQ(rhs TimeExpression) BoolExpression
}

type timeInterfaceImpl struct {
	parent TimeExpression
}

func (t *timeInterfaceImpl) EQ(rhs TimeExpression) BoolExpression {
	return EQ(t.parent, rhs)
}

func (t *timeInterfaceImpl) NOT_EQ(rhs TimeExpression) BoolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *timeInterfaceImpl) IS_DISTINCT_FROM(rhs TimeExpression) BoolExpression {
	return IS_DISTINCT_FROM(t.parent, rhs)
}

func (t *timeInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimeExpression) BoolExpression {
	return IS_NOT_DISTINCT_FROM(t.parent, rhs)
}

func (t *timeInterfaceImpl) LT(rhs TimeExpression) BoolExpression {
	return LT(t.parent, rhs)
}

func (t *timeInterfaceImpl) LT_EQ(rhs TimeExpression) BoolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *timeInterfaceImpl) GT(rhs TimeExpression) BoolExpression {
	return GT(t.parent, rhs)
}

func (t *timeInterfaceImpl) GT_EQ(rhs TimeExpression) BoolExpression {
	return GT_EQ(t.parent, rhs)
}

//---------------------------------------------------//
type prefixTimeExpression struct {
	expressionInterfaceImpl
	timeInterfaceImpl

	prefixOpExpression
}

func newPrefixTimeExpression(operator string, expression Expression) TimeExpression {
	timeExpr := prefixTimeExpression{}
	timeExpr.prefixOpExpression = newPrefixExpression(expression, operator)

	timeExpr.expressionInterfaceImpl.parent = &timeExpr
	timeExpr.timeInterfaceImpl.parent = &timeExpr

	return &timeExpr
}

func INTERVAL(interval string) Expression {
	return newPrefixTimeExpression("INTERVAL", literal(interval))
}

//---------------------------------------------------//

type timeExpressionWrapper struct {
	timeInterfaceImpl
	Expression
}

func newTimeExpressionWrap(expression Expression) TimeExpression {
	timeExpressionWrap := timeExpressionWrapper{Expression: expression}
	timeExpressionWrap.timeInterfaceImpl.parent = &timeExpressionWrap
	return &timeExpressionWrap
}

func TimeExp(expression Expression) TimeExpression {
	return newTimeExpressionWrap(expression)
}
