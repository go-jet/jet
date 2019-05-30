package sqlbuilder

import "fmt"

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

func Time(hour, minute, second, milliseconds int) timeExpression {
	timeLiteral := timeLiteral{}
	timeStr := fmt.Sprintf("%02d:%02d:%02d.%03d", hour, minute, second, milliseconds)
	timeLiteral.literalExpression = *Literal(timeStr)

	timeLiteral.timeInterfaceImpl.parent = &timeLiteral

	return &timeLiteral
}

//---------------------------------------------------//
type timezLiteral struct {
	timezInterfaceImpl
	literalExpression
}

func Timez(hour, minute, second, milliseconds, timezone int) timezExpression {
	timezLiteral := timezLiteral{}
	timeStr := fmt.Sprintf("%02d:%02d:%02d.%03d %+03d", hour, minute, second, milliseconds, timezone)
	timezLiteral.literalExpression = *Literal(timeStr)

	timezLiteral.timezInterfaceImpl.parent = &timezLiteral

	return &timezLiteral
}

//---------------------------------------------------//
type timestampLiteral struct {
	timestampInterfaceImpl
	literalExpression
}

func Timestamp(year, month, day, hour, minute, second, milliseconds int) TimestampExpression {
	timestampLiteral := timestampLiteral{}
	timeStr := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%03d", year, month, day, hour, minute, second, milliseconds)
	timestampLiteral.literalExpression = *Literal(timeStr)

	timestampLiteral.timestampInterfaceImpl.parent = &timestampLiteral

	return &timestampLiteral
}

//---------------------------------------------------//
type timestampzLiteral struct {
	timestampzInterfaceImpl
	literalExpression
}

func Timestampz(year, month, day, hour, minute, second, milliseconds, timezone int) TimestampzExpression {
	timestampzLiteral := timestampzLiteral{}
	timeStr := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%03d %+04d",
		year, month, day, hour, minute, second, milliseconds, timezone)

	timestampzLiteral.literalExpression = *Literal(timeStr)

	timestampzLiteral.timestampzInterfaceImpl.parent = &timestampzLiteral

	return &timestampzLiteral
}

//---------------------------------------------------//
type dateLiteral struct {
	dateInterfaceImpl
	literalExpression
}

func Date(year, month, day int) DateExpression {
	dateLiteral := dateLiteral{}

	timeStr := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	dateLiteral.literalExpression = *Literal(timeStr)
	dateLiteral.dateInterfaceImpl.parent = &dateLiteral

	return &dateLiteral
}
