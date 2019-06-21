package jet

type cast struct {
	Expression
	castType string
}

func newCast(expression Expression, castType string) *cast {
	return &cast{
		Expression: expression,
		castType:   castType,
	}
}

func (b *cast) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	err := b.Expression.serialize(statement, out, options...)
	out.writeString("::" + b.castType)
	return err
}

type boolCast struct {
	expressionInterfaceImpl
	boolInterfaceImpl
	cast
}

func newBoolCast(expression Expression) BoolExpression {
	boolCast := &boolCast{cast: *newCast(expression, "boolean")}

	boolCast.boolInterfaceImpl.parent = boolCast
	boolCast.expressionInterfaceImpl.parent = boolCast

	return boolCast
}

type integerCast struct {
	expressionInterfaceImpl
	integerInterfaceImpl
	cast
}

func newIntegerCast(expression Expression, intType string) IntegerExpression {
	integerCast := &integerCast{cast: *newCast(expression, intType)}

	integerCast.integerInterfaceImpl.parent = integerCast
	integerCast.expressionInterfaceImpl.parent = integerCast

	return integerCast
}

type floatCast struct {
	expressionInterfaceImpl
	floatInterfaceImpl
	cast
}

func newFloatCast(expression Expression, floatType string) FloatExpression {
	floatCast := &floatCast{cast: *newCast(expression, floatType)}

	floatCast.floatInterfaceImpl.parent = floatCast
	floatCast.expressionInterfaceImpl.parent = floatCast

	return floatCast
}

type textCast struct {
	expressionInterfaceImpl
	stringInterfaceImpl
	cast
}

func newTextCast(expression Expression) StringExpression {
	textCast := &textCast{cast: *newCast(expression, "text")}

	textCast.stringInterfaceImpl.parent = textCast
	textCast.expressionInterfaceImpl.parent = textCast

	return textCast
}

type dateCast struct {
	expressionInterfaceImpl
	dateInterfaceImpl
	cast
}

func newDateCast(expression Expression) DateExpression {
	dateCast := &dateCast{cast: *newCast(expression, "date")}

	dateCast.dateInterfaceImpl.parent = dateCast
	dateCast.expressionInterfaceImpl.parent = dateCast

	return dateCast
}

type timeCast struct {
	expressionInterfaceImpl
	timeInterfaceImpl
	cast
}

func newTimeCast(expression Expression) TimeExpression {
	timeCast := &timeCast{cast: *newCast(expression, "time without time zone")}

	timeCast.timeInterfaceImpl.parent = timeCast
	timeCast.expressionInterfaceImpl.parent = timeCast

	return timeCast
}

type timezCast struct {
	expressionInterfaceImpl
	timezInterfaceImpl
	cast
}

func newTimezCast(expression Expression) TimezExpression {
	timezCast := &timezCast{cast: *newCast(expression, "time with time zone")}

	timezCast.timezInterfaceImpl.parent = timezCast
	timezCast.expressionInterfaceImpl.parent = timezCast

	return timezCast
}

type timestampCast struct {
	expressionInterfaceImpl
	timestampInterfaceImpl
	cast
}

func newTimestampCast(expression Expression) TimestampExpression {
	timestampCast := &timestampCast{cast: *newCast(expression, "timestamp without time zone")}

	timestampCast.timestampInterfaceImpl.parent = timestampCast
	timestampCast.expressionInterfaceImpl.parent = timestampCast

	return timestampCast
}

type timestampzCast struct {
	expressionInterfaceImpl
	timestampzInterfaceImpl
	cast
}

func newTimestampzCast(expression Expression) TimestampzExpression {
	timestampzCast := &timestampzCast{cast: *newCast(expression, "timestamp with time zone")}

	timestampzCast.timestampzInterfaceImpl.parent = timestampzCast
	timestampzCast.expressionInterfaceImpl.parent = timestampzCast

	return timestampzCast
}
