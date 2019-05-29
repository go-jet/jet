package sqlbuilder

import "time"

// Representation of an escaped literal
type literalExpression struct {
	expressionInterfaceImpl
	value interface{}
}

func Literal(value interface{}) *literalExpression {
	exp := literalExpression{value: value}
	exp.expressionInterfaceImpl.parent = &exp

	return &exp
}

func (l literalExpression) serialize(statement statementType, out *queryData) error {
	out.insertArgument(l.value)

	return nil
}

type numLiteralExpression struct {
	literalExpression
	numericInterfaceImpl
}

func Int(value int) numericExpression {
	numLiteral := &numLiteralExpression{}

	numLiteral.literalExpression = *Literal(value)
	numLiteral.literalExpression.parent = numLiteral

	numLiteral.numericInterfaceImpl.parent = numLiteral

	return numLiteral
}

//---------------------------------------------------//
type boolLiteralExpression struct {
	boolInterfaceImpl
	literalExpression
}

func Bool(value bool) boolExpression {
	boolLiteralExpression := boolLiteralExpression{}

	boolLiteralExpression.literalExpression = *Literal(value)
	boolLiteralExpression.boolInterfaceImpl.parent = &boolLiteralExpression

	return &boolLiteralExpression
}

//---------------------------------------------------//
type numericLiteral struct {
	numericInterfaceImpl
	literalExpression
}

func Float(value float64) numericExpression {
	numericLiteral := numericLiteral{}
	numericLiteral.literalExpression = *Literal(value)

	numericLiteral.numericInterfaceImpl.parent = &numericLiteral

	return &numericLiteral
}

//---------------------------------------------------//
type stringLiteral struct {
	stringInterfaceImpl
	literalExpression
}

func String(value string) stringExpression {
	stringLiteral := stringLiteral{}
	stringLiteral.literalExpression = *Literal(value)

	stringLiteral.stringInterfaceImpl.parent = &stringLiteral

	return &stringLiteral
}

//---------------------------------------------------//
type timeLiteral struct {
	timeInterfaceImpl
	literalExpression
}

func Time(value time.Time) timeExpression {
	timeLiteral := timeLiteral{}
	timeLiteral.literalExpression = *Literal(value)

	timeLiteral.timeInterfaceImpl.parent = &timeLiteral

	return &timeLiteral
}
