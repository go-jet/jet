package sqlbuilder

type StringExpression interface {
	Expression

	Eq(expression StringExpression) BoolExpression
	EqString(value string) BoolExpression
	NotEq(expression StringExpression) BoolExpression
	NotEqString(value string) BoolExpression
}

type stringInterfaceImpl struct {
	parent StringExpression
}

func (b *stringInterfaceImpl) Eq(expression StringExpression) BoolExpression {
	return Eq(b.parent, expression)
}

func (b *stringInterfaceImpl) EqString(value string) BoolExpression {
	return EqL(b.parent, value)
}

func (b *stringInterfaceImpl) NotEq(expression StringExpression) BoolExpression {
	return NotEq(b.parent, expression)
}

func (b *stringInterfaceImpl) NotEqString(value string) BoolExpression {
	return NotEq(b.parent, Literal(value))
}
