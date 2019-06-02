package sqlbuilder

type cast struct {
	expression expression
	castType   string
}

func newCast(expression expression, castType string) cast {
	return cast{
		expression: expression,
		castType:   castType,
	}
}

func (b *cast) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	err := b.expression.serialize(statement, out, options...)
	out.writeString("::" + b.castType)
	return err
}

type boolCast struct {
	expressionInterfaceImpl
	boolInterfaceImpl
	cast
}

func newBoolCast(expression expression) BoolExpression {
	boolCast := &boolCast{cast: newCast(expression, "boolean")}

	boolCast.boolInterfaceImpl.parent = boolCast
	boolCast.expressionInterfaceImpl.parent = boolCast

	return boolCast
}

type integerCast struct {
	expressionInterfaceImpl
	integerInterfaceImpl
	cast
}

func newIntegerCast(expression expression) IntegerExpression {
	integerCast := &integerCast{cast: newCast(expression, "integer")}

	integerCast.integerInterfaceImpl.parent = integerCast
	integerCast.expressionInterfaceImpl.parent = integerCast

	return integerCast
}

type floatCast struct {
	expressionInterfaceImpl
	floatInterfaceImpl
	cast
}

func newDoubleCast(expression expression) FloatExpression {
	floatCast := &floatCast{cast: newCast(expression, "double precision")}

	floatCast.floatInterfaceImpl.parent = floatCast
	floatCast.expressionInterfaceImpl.parent = floatCast

	return floatCast
}

type textCast struct {
	expressionInterfaceImpl
	stringInterfaceImpl
	cast
}

func newTextCast(expression expression) StringExpression {
	textCast := &textCast{cast: newCast(expression, "text")}

	textCast.stringInterfaceImpl.parent = textCast
	textCast.expressionInterfaceImpl.parent = textCast

	return textCast
}

type dateCast struct {
	expressionInterfaceImpl
	dateInterfaceImpl
	cast
}

func newDateCast(expression expression) DateExpression {
	dateCast := &dateCast{cast: newCast(expression, "date")}

	dateCast.dateInterfaceImpl.parent = dateCast
	dateCast.expressionInterfaceImpl.parent = dateCast

	return dateCast
}

type timeCast struct {
	expressionInterfaceImpl
	timeInterfaceImpl
	cast
}

func newTimeCast(expression expression) TimeExpression {
	timeCast := &timeCast{cast: newCast(expression, "time without time zone")}

	timeCast.timeInterfaceImpl.parent = timeCast
	timeCast.expressionInterfaceImpl.parent = timeCast

	return timeCast
}

type timezCast struct {
	expressionInterfaceImpl
	timezInterfaceImpl
	cast
}

func newTimezCast(expression expression) TimezExpression {
	timezCast := &timezCast{cast: newCast(expression, "time with time zone")}

	timezCast.timezInterfaceImpl.parent = timezCast
	timezCast.expressionInterfaceImpl.parent = timezCast

	return timezCast
}

type timestampCast struct {
	expressionInterfaceImpl
	timestampInterfaceImpl
	cast
}

func newTimestampCast(expression expression) TimestampExpression {
	timestampCast := &timestampCast{cast: newCast(expression, "timestamp without time zone")}

	timestampCast.timestampInterfaceImpl.parent = timestampCast
	timestampCast.expressionInterfaceImpl.parent = timestampCast

	return timestampCast
}

type timestampzCast struct {
	expressionInterfaceImpl
	timestampzInterfaceImpl
	cast
}

func newTimestampzCast(expression expression) TimestampzExpression {
	timestampzCast := &timestampzCast{cast: newCast(expression, "timestamp with time zone")}

	timestampzCast.timestampzInterfaceImpl.parent = timestampzCast
	timestampzCast.expressionInterfaceImpl.parent = timestampzCast

	return timestampzCast
}
