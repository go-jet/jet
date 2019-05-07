package sqlbuilder

type stringExpression interface {
	expression

	Eq(expression stringExpression) boolExpression
	EqString(value string) boolExpression
	NotEq(expression stringExpression) boolExpression
	NotEqString(value string) boolExpression
}

type stringInterfaceImpl struct {
	parent stringExpression
}

func (b *stringInterfaceImpl) Eq(expression stringExpression) boolExpression {
	return Eq(b.parent, expression)
}

func (b *stringInterfaceImpl) EqString(value string) boolExpression {
	return EqL(b.parent, value)
}

func (b *stringInterfaceImpl) NotEq(expression stringExpression) boolExpression {
	return NotEq(b.parent, expression)
}

func (b *stringInterfaceImpl) NotEqString(value string) boolExpression {
	return NotEq(b.parent, Literal(value))
}
