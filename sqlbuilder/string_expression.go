package sqlbuilder

type StringExpression interface {
	Expression

	Eq(expression StringExpression) BoolExpression
	EqL(value string) BoolExpression
	NotEq(expression StringExpression) BoolExpression
	NotEqL(value string) BoolExpression
}

type stringInterfaceImpl struct {
	parent StringExpression
}

func (b *stringInterfaceImpl) Eq(expression StringExpression) BoolExpression {
	return newBinaryBoolExpression(b.parent, expression, " = ")
}

func (b *stringInterfaceImpl) EqL(value string) BoolExpression {
	return newBinaryBoolExpression(b.parent, Literal(value), " = ")
}

func (b *stringInterfaceImpl) NotEq(expression StringExpression) BoolExpression {
	return newBinaryBoolExpression(b.parent, expression, " != ")
}

func (b *stringInterfaceImpl) NotEqL(value string) BoolExpression {
	return newBinaryBoolExpression(b.parent, Literal(value), " != ")
}
