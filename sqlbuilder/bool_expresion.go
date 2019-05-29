package sqlbuilder

type boolExpression interface {
	expression

	EQ(expression boolExpression) boolExpression
	NOT_EQ(expression boolExpression) boolExpression

	IS_TRUE() boolExpression
	IS_NOT_TRUE() boolExpression
	IS_FALSE() boolExpression
	IS_NOT_FALSE() boolExpression
	IS_UNKNOWN() boolExpression
	IS_NOT_UNKNOWN() boolExpression

	AND(expression boolExpression) boolExpression
	OR(expression boolExpression) boolExpression
}

type boolInterfaceImpl struct {
	parent boolExpression
}

func (b *boolInterfaceImpl) EQ(expression boolExpression) boolExpression {
	return EQ(b.parent, expression)
}

func (b *boolInterfaceImpl) NOT_EQ(expression boolExpression) boolExpression {
	return NOT_EQ(b.parent, expression)
}

func (b *boolInterfaceImpl) AND(expression boolExpression) boolExpression {
	return And(b.parent, expression)
}

func (b *boolInterfaceImpl) OR(expression boolExpression) boolExpression {
	return Or(b.parent, expression)
}

func (b *boolInterfaceImpl) IS_TRUE() boolExpression {
	return IS_TRUE(b.parent)
}

func (b *boolInterfaceImpl) IS_NOT_TRUE() boolExpression {
	return IS_NOT_TRUE(b.parent)
}

func (b *boolInterfaceImpl) IS_FALSE() boolExpression {
	return IS_FALSE(b.parent)
}

func (b *boolInterfaceImpl) IS_NOT_FALSE() boolExpression {
	return IS_NOT_FALSE(b.parent)
}

func (b *boolInterfaceImpl) IS_UNKNOWN() boolExpression {
	return IS_UNKNOWN(b.parent)
}

func (b *boolInterfaceImpl) IS_NOT_UNKNOWN() boolExpression {
	return IS_NOT_UNKNOWN(b.parent)
}

//---------------------------------------------------//
type binaryBoolExpression struct {
	expressionInterfaceImpl
	boolInterfaceImpl

	binaryOpExpression
}

func newBinaryBoolExpression(lhs, rhs expression, operator string) boolExpression {
	boolExpression := binaryBoolExpression{}

	boolExpression.binaryOpExpression = newBinaryExpression(lhs, rhs, operator)
	boolExpression.expressionInterfaceImpl.parent = &boolExpression
	boolExpression.boolInterfaceImpl.parent = &boolExpression

	return &boolExpression
}

//---------------------------------------------------//
type prefixBoolExpression struct {
	expressionInterfaceImpl
	boolInterfaceImpl

	prefixOpExpression
}

func newPrefixBoolExpression(expression expression, operator string) boolExpression {
	exp := prefixBoolExpression{}
	exp.prefixOpExpression = newPrefixExpression(expression, operator)

	exp.expressionInterfaceImpl.parent = &exp
	exp.boolInterfaceImpl.parent = &exp

	return &exp
}

//---------------------------------------------------//
type postfixBoolOpExpression struct {
	expressionInterfaceImpl
	boolInterfaceImpl

	postfixOpExpression
}

func newPostifxBoolExpression(expression expression, operator string) boolExpression {
	exp := postfixBoolOpExpression{}
	exp.postfixOpExpression = newPostfixOpExpression(expression, operator)

	exp.expressionInterfaceImpl.parent = &exp
	exp.boolInterfaceImpl.parent = &exp

	return &exp
}
