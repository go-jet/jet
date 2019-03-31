package sqlbuilder

type IntegerExpression interface {
	NumericExpression

	//AddInt(value int) IntegerExpression
	//AddInt64(value int) IntegerExpression

	BitAnd(expression IntegerExpression) IntegerExpression
	BitOr(expression IntegerExpression) IntegerExpression
	BitXor(expression IntegerExpression) IntegerExpression
	BitNot() IntegerExpression
}

type integerInterfaceImpl struct {
	parent IntegerExpression
}

//func (i *integerInterfaceImpl) AddInt(expression IntegerExpression) IntegerExpression {
//	return NewBinaryIntegerExpression(i.parent, expression, " & ")
//}
//
//func (i *integerInterfaceImpl) AddInt64(expression IntegerExpression) IntegerExpression {
//	return NewBinaryIntegerExpression(i.parent, expression, " & ")
//}

func (i *integerInterfaceImpl) BitAnd(expression IntegerExpression) IntegerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, " & ")
}

func (i *integerInterfaceImpl) BitOr(expression IntegerExpression) IntegerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, " | ")
}

func (i *integerInterfaceImpl) BitXor(expression IntegerExpression) IntegerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, " # ")
}

func (i *integerInterfaceImpl) BitNot() IntegerExpression {
	return NewPrefixIntegerExpression(i.parent, " ~")
}

//---------------------------------------------------//
type binaryIntegerExpression struct {
	expressionInterfaceImpl
	numericInterfaceImpl
	integerInterfaceImpl

	binaryExpression
}

func NewBinaryIntegerExpression(lhs, rhs IntegerExpression, operator string) IntegerExpression {
	integerExpression := binaryIntegerExpression{}

	integerExpression.expressionInterfaceImpl.parent = &integerExpression
	integerExpression.numericInterfaceImpl.parent = &integerExpression
	integerExpression.integerInterfaceImpl.parent = &integerExpression

	integerExpression.binaryExpression = newBinaryExpression(lhs, rhs, []byte(operator))

	return &integerExpression
}

//---------------------------------------------------//
type prefixIntegerExpression struct {
	expressionInterfaceImpl
	numericInterfaceImpl
	integerInterfaceImpl

	prefixExpression
}

func NewPrefixIntegerExpression(expression IntegerExpression, operator string) IntegerExpression {
	integerExpression := prefixIntegerExpression{}
	integerExpression.prefixExpression = newPrefixExpression(expression, []byte(operator))

	integerExpression.expressionInterfaceImpl.parent = &integerExpression
	integerExpression.numericInterfaceImpl.parent = &integerExpression
	integerExpression.integerInterfaceImpl.parent = &integerExpression

	return &integerExpression
}
