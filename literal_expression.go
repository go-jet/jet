package jet

import "fmt"

// Representation of an escaped literal
type literalExpression struct {
	expressionInterfaceImpl
	value    interface{}
	constant bool
}

func literal(value interface{}) *literalExpression {
	exp := literalExpression{value: value}
	exp.expressionInterfaceImpl.parent = &exp

	return &exp
}

func constLiteral(value interface{}) *literalExpression {
	exp := literal(value)
	exp.constant = true

	return exp
}

func (l literalExpression) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	if l.constant {
		out.insertConstantArgument(l.value)
	} else {
		out.insertParametrizedArgument(l.value)
	}

	return nil
}

type integerLiteralExpression struct {
	literalExpression
	integerInterfaceImpl
}

// Int is constructor for integer expressions literals.
func Int(value int64) IntegerExpression {
	numLiteral := &integerLiteralExpression{}

	numLiteral.literalExpression = *literal(value)

	numLiteral.literalExpression.parent = numLiteral
	numLiteral.integerInterfaceImpl.parent = numLiteral

	return numLiteral
}

//---------------------------------------------------//
type boolLiteralExpression struct {
	boolInterfaceImpl
	literalExpression
}

// Bool creates new bool literal expression
func Bool(value bool) BoolExpression {
	boolLiteralExpression := boolLiteralExpression{}

	boolLiteralExpression.literalExpression = *literal(value)
	boolLiteralExpression.boolInterfaceImpl.parent = &boolLiteralExpression

	return &boolLiteralExpression
}

//---------------------------------------------------//
type floatLiteral struct {
	floatInterfaceImpl
	literalExpression
}

// Float creates new float literal expression
func Float(value float64) FloatExpression {
	floatLiteral := floatLiteral{}
	floatLiteral.literalExpression = *literal(value)

	floatLiteral.floatInterfaceImpl.parent = &floatLiteral

	return &floatLiteral
}

//---------------------------------------------------//
type stringLiteral struct {
	stringInterfaceImpl
	literalExpression
}

// String creates new string literal expression
func String(value string) StringExpression {
	stringLiteral := stringLiteral{}
	stringLiteral.literalExpression = *literal(value)

	stringLiteral.stringInterfaceImpl.parent = &stringLiteral

	return &stringLiteral
}

//---------------------------------------------------//
type timeLiteral struct {
	timeInterfaceImpl
	literalExpression
}

// Time creates new time literal expression
func Time(hour, minute, second, milliseconds int) TimeExpression {
	timeLiteral := &timeLiteral{}
	timeStr := fmt.Sprintf("%02d:%02d:%02d.%03d", hour, minute, second, milliseconds)
	timeLiteral.literalExpression = *literal(timeStr)

	timeLiteral.timeInterfaceImpl.parent = timeLiteral

	return CAST(timeLiteral).AS_TIME()
}

//---------------------------------------------------//
type timezLiteral struct {
	timezInterfaceImpl
	literalExpression
}

// Timez creates new time with time zone literal expression
func Timez(hour, minute, second, milliseconds, timezone int) TimezExpression {
	timezLiteral := &timezLiteral{}
	timeStr := fmt.Sprintf("%02d:%02d:%02d.%03d %+03d", hour, minute, second, milliseconds, timezone)
	timezLiteral.literalExpression = *literal(timeStr)

	timezLiteral.timezInterfaceImpl.parent = timezLiteral

	return CAST(timezLiteral).AS_TIMEZ()
}

//---------------------------------------------------//
type timestampLiteral struct {
	timestampInterfaceImpl
	literalExpression
}

// Timestamp creates new timestamp literal expression
func Timestamp(year, month, day, hour, minute, second, milliseconds int) TimestampExpression {
	timestampLiteral := &timestampLiteral{}
	timeStr := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%03d", year, month, day, hour, minute, second, milliseconds)
	timestampLiteral.literalExpression = *literal(timeStr)

	timestampLiteral.timestampInterfaceImpl.parent = timestampLiteral

	return CAST(timestampLiteral).AS_TIMESTAMP()
}

//---------------------------------------------------//
type timestampzLiteral struct {
	timestampzInterfaceImpl
	literalExpression
}

// Timestampz creates new timestamp with time zone literal expression
func Timestampz(year, month, day, hour, minute, second, milliseconds, timezone int) TimestampzExpression {
	timestampzLiteral := &timestampzLiteral{}
	timeStr := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%03d %+04d",
		year, month, day, hour, minute, second, milliseconds, timezone)

	timestampzLiteral.literalExpression = *literal(timeStr)

	timestampzLiteral.timestampzInterfaceImpl.parent = timestampzLiteral

	return CAST(timestampzLiteral).AS_TIMESTAMPZ()
}

//---------------------------------------------------//
type dateLiteral struct {
	dateInterfaceImpl
	literalExpression
}

//Date creates new date expression
func Date(year, month, day int) DateExpression {
	dateLiteral := &dateLiteral{}

	timeStr := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	dateLiteral.literalExpression = *literal(timeStr)
	dateLiteral.dateInterfaceImpl.parent = dateLiteral

	return CAST(dateLiteral).AS_DATE()
}

//--------------------------------------------------//
type nullLiteral struct {
	expressionInterfaceImpl
}

func newNullLiteral() Expression {
	nullExpression := &nullLiteral{}

	nullExpression.expressionInterfaceImpl.parent = nullExpression

	return nullExpression
}

func (n *nullLiteral) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	out.writeString("NULL")
	return nil
}

//--------------------------------------------------//
type starLiteral struct {
	expressionInterfaceImpl
}

func newStarLiteral() Expression {
	starExpression := &starLiteral{}

	starExpression.expressionInterfaceImpl.parent = starExpression

	return starExpression
}

func (n *starLiteral) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	out.writeString("*")
	return nil
}

//---------------------------------------------------//

type wrap struct {
	expressionInterfaceImpl
	expressions []Expression
}

func (n *wrap) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	out.writeString("(")
	err := serializeExpressionList(statement, n.expressions, ", ", out)
	out.writeString(")")
	return err
}

// WRAP wraps list of expressions with brackets '(' and ')'
func WRAP(expression ...Expression) Expression {
	wrap := &wrap{expressions: expression}
	wrap.expressionInterfaceImpl.parent = wrap

	return wrap
}

//---------------------------------------------------//

type rawExpression struct {
	expressionInterfaceImpl
	raw string
}

func (n *rawExpression) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	out.writeString(n.raw)
	return nil
}

// RAW can be used for any unsupported functions, operators or expressions.
// For example: RAW("current_database()")
func RAW(raw string) Expression {
	rawExp := &rawExpression{raw: raw}
	rawExp.expressionInterfaceImpl.parent = rawExp

	return rawExp
}
