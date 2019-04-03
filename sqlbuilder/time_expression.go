package sqlbuilder

type TimeExpression interface {
	Expression

	Eq(expression TimeExpression) BoolExpression
	EqL(literal string) BoolExpression
	NotEq(expression TimeExpression) BoolExpression
	NotEqL(literal string) BoolExpression
	GtEq(rhs TimeExpression) BoolExpression
	GtEqL(literal string) BoolExpression
	LtEq(rhs TimeExpression) BoolExpression
	LtEqL(literal string) BoolExpression
}

type timeInterfaceImpl struct {
	parent TimeExpression
}

func (t *timeInterfaceImpl) Eq(expression TimeExpression) BoolExpression {
	return Eq(t.parent, expression)
}

func (t *timeInterfaceImpl) EqL(literal string) BoolExpression {
	return Eq(t.parent, Literal(literal))
}

func (t *timeInterfaceImpl) NotEq(expression TimeExpression) BoolExpression {
	return NotEq(t.parent, expression)
}

func (t *timeInterfaceImpl) NotEqL(literal string) BoolExpression {
	return NotEq(t.parent, Literal(literal))
}

func (t *timeInterfaceImpl) GtEq(expression TimeExpression) BoolExpression {
	return GtEq(t.parent, expression)
}

func (t *timeInterfaceImpl) GtEqL(literal string) BoolExpression {
	return GtEq(t.parent, Literal(literal))
}

func (t *timeInterfaceImpl) LtEq(expression TimeExpression) BoolExpression {
	return LtEq(t.parent, expression)
}

func (t *timeInterfaceImpl) LtEqL(literal string) BoolExpression {
	return LtEq(t.parent, Literal(literal))
}
