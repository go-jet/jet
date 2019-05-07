package sqlbuilder

type timeExpression interface {
	expression

	Eq(expression timeExpression) boolExpression
	EqL(literal string) boolExpression
	NotEq(expression timeExpression) boolExpression
	NotEqL(literal string) boolExpression
	GtEq(rhs timeExpression) boolExpression
	GtEqL(literal string) boolExpression
	LtEq(rhs timeExpression) boolExpression
	LtEqL(literal string) boolExpression
}

type timeInterfaceImpl struct {
	parent timeExpression
}

func (t *timeInterfaceImpl) Eq(expression timeExpression) boolExpression {
	return Eq(t.parent, expression)
}

func (t *timeInterfaceImpl) EqL(literal string) boolExpression {
	return Eq(t.parent, Literal(literal))
}

func (t *timeInterfaceImpl) NotEq(expression timeExpression) boolExpression {
	return NotEq(t.parent, expression)
}

func (t *timeInterfaceImpl) NotEqL(literal string) boolExpression {
	return NotEq(t.parent, Literal(literal))
}

func (t *timeInterfaceImpl) GtEq(expression timeExpression) boolExpression {
	return GtEq(t.parent, expression)
}

func (t *timeInterfaceImpl) GtEqL(literal string) boolExpression {
	return GtEq(t.parent, Literal(literal))
}

func (t *timeInterfaceImpl) LtEq(expression timeExpression) boolExpression {
	return LtEq(t.parent, expression)
}

func (t *timeInterfaceImpl) LtEqL(literal string) boolExpression {
	return LtEq(t.parent, Literal(literal))
}

//---------------------------------------------------//
type prefixTimeExpression struct {
	expressionInterfaceImpl
	timeInterfaceImpl

	prefixExpression
}

func newPrefixTimeExpression(expression expression, operator string) timeExpression {
	timeExpr := prefixTimeExpression{}
	timeExpr.prefixExpression = newPrefixExpression(expression, operator)

	timeExpr.expressionInterfaceImpl.parent = &timeExpr
	timeExpr.timeInterfaceImpl.parent = &timeExpr

	return &timeExpr
}

func INTERVAL(interval string) expression {
	return newPrefixTimeExpression(Literal(interval), "INTERVAL")
}
