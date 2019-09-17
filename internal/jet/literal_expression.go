package jet

import (
	"fmt"
	"time"
)

// LiteralExpression is representation of an escaped literal
type LiteralExpression interface {
	Expression

	Value() interface{}
	SetConstant(constant bool)
}

type literalExpressionImpl struct {
	expressionInterfaceImpl

	value    interface{}
	constant bool
}

func literal(value interface{}, optionalConstant ...bool) *literalExpressionImpl {
	exp := literalExpressionImpl{value: value}

	if len(optionalConstant) > 0 {
		exp.constant = optionalConstant[0]
	}

	exp.expressionInterfaceImpl.Parent = &exp

	return &exp
}

// FixedLiteral is injected directly to SQL query, and does not appear in parametrized argument list.
func FixedLiteral(value interface{}) *literalExpressionImpl {
	exp := literal(value)
	exp.constant = true

	return exp
}

func (l *literalExpressionImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if l.constant {
		out.insertConstantArgument(l.value)
	} else {
		out.insertParametrizedArgument(l.value)
	}
}

func (l *literalExpressionImpl) Value() interface{} {
	return l.value
}

func (l *literalExpressionImpl) SetConstant(constant bool) {
	l.constant = constant
}

type integerLiteralExpression struct {
	literalExpressionImpl
	integerInterfaceImpl
}

// Int creates new integer literal
func Int(value int64) IntegerExpression {
	numLiteral := &integerLiteralExpression{}

	numLiteral.literalExpressionImpl = *literal(value)

	numLiteral.literalExpressionImpl.Parent = numLiteral
	numLiteral.integerInterfaceImpl.parent = numLiteral

	return numLiteral
}

//---------------------------------------------------//
type boolLiteralExpression struct {
	boolInterfaceImpl
	literalExpressionImpl
}

// Bool creates new bool literal expression
func Bool(value bool) BoolExpression {
	boolLiteralExpression := boolLiteralExpression{}

	boolLiteralExpression.literalExpressionImpl = *literal(value)
	boolLiteralExpression.boolInterfaceImpl.parent = &boolLiteralExpression

	return &boolLiteralExpression
}

//---------------------------------------------------//
type floatLiteral struct {
	floatInterfaceImpl
	literalExpressionImpl
}

// Float creates new float literal
func Float(value float64) FloatExpression {
	floatLiteral := floatLiteral{}
	floatLiteral.literalExpressionImpl = *literal(value)

	floatLiteral.floatInterfaceImpl.parent = &floatLiteral

	return &floatLiteral
}

//---------------------------------------------------//
type stringLiteral struct {
	stringInterfaceImpl
	literalExpressionImpl
}

// String creates new string literal expression
func String(value string) StringExpression {
	stringLiteral := stringLiteral{}
	stringLiteral.literalExpressionImpl = *literal(value)

	stringLiteral.stringInterfaceImpl.parent = &stringLiteral

	return &stringLiteral
}

//---------------------------------------------------//

type timeLiteral struct {
	timeInterfaceImpl
	literalExpressionImpl
}

// Time creates new time literal expression
func Time(hour, minute, second int, nanoseconds ...time.Duration) TimeExpression {
	timeLiteral := &timeLiteral{}
	timeStr := fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
	timeStr += formatNanoseconds(nanoseconds...)
	timeLiteral.literalExpressionImpl = *literal(timeStr)

	timeLiteral.timeInterfaceImpl.parent = timeLiteral

	return timeLiteral
}

// TimeT creates new time literal expression from time.Time object
func TimeT(t time.Time) TimeExpression {
	timeLiteral := &timeLiteral{}
	timeLiteral.literalExpressionImpl = *literal(t)
	timeLiteral.timeInterfaceImpl.parent = timeLiteral

	return timeLiteral
}

//---------------------------------------------------//

type timezLiteral struct {
	timezInterfaceImpl
	literalExpressionImpl
}

// Timez creates new time with time zone literal expression
func Timez(hour, minute, second int, nanoseconds time.Duration, timezone string) TimezExpression {
	timezLiteral := timezLiteral{}
	timeStr := fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
	timeStr += formatNanoseconds(nanoseconds)
	timeStr += " " + timezone
	timezLiteral.literalExpressionImpl = *literal(timeStr)

	return TimezExp(literal(timeStr))
}

// TimezT creates new time with time zone literal expression from time.Time object
func TimezT(t time.Time) TimezExpression {
	timeLiteral := &timezLiteral{}
	timeLiteral.literalExpressionImpl = *literal(t)
	timeLiteral.timezInterfaceImpl.parent = timeLiteral

	return timeLiteral
}

//---------------------------------------------------//

type timestampLiteral struct {
	timestampInterfaceImpl
	literalExpressionImpl
}

// Timestamp creates new timestamp literal expression
func Timestamp(year int, month time.Month, day, hour, minute, second int, nanoseconds ...time.Duration) TimestampExpression {
	timestamp := &timestampLiteral{}
	timeStr := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	timeStr += formatNanoseconds(nanoseconds...)
	timestamp.literalExpressionImpl = *literal(timeStr)
	timestamp.timestampInterfaceImpl.parent = timestamp
	return timestamp
}

// TimestampT creates new timestamp literal expression from time.Time object
func TimestampT(t time.Time) TimestampExpression {
	timestamp := &timestampLiteral{}
	timestamp.literalExpressionImpl = *literal(t)
	timestamp.timestampInterfaceImpl.parent = timestamp
	return timestamp
}

//---------------------------------------------------//

type timestampzLiteral struct {
	timestampzInterfaceImpl
	literalExpressionImpl
}

// Timestampz creates new timestamp with time zone literal expression
func Timestampz(year int, month time.Month, day, hour, minute, second int, nanoseconds time.Duration, timezone string) TimestampzExpression {
	timestamp := &timestampzLiteral{}
	timeStr := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	timeStr += formatNanoseconds(nanoseconds)
	timeStr += " " + timezone

	timestamp.literalExpressionImpl = *literal(timeStr)
	timestamp.timestampzInterfaceImpl.parent = timestamp
	return timestamp
}

// TimestampzT creates new timestamp literal expression from time.Time object
func TimestampzT(t time.Time) TimestampzExpression {
	timestamp := &timestampzLiteral{}
	timestamp.literalExpressionImpl = *literal(t)
	timestamp.timestampzInterfaceImpl.parent = timestamp
	return timestamp
}

//---------------------------------------------------//

type dateLiteral struct {
	dateInterfaceImpl
	literalExpressionImpl
}

// Date creates new date literal expression
func Date(year int, month time.Month, day int) DateExpression {
	dateLiteral := &dateLiteral{}

	timeStr := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	dateLiteral.literalExpressionImpl = *literal(timeStr)
	dateLiteral.dateInterfaceImpl.parent = dateLiteral

	return dateLiteral
}

// DateT creates new date literal expression from time.Time object
func DateT(t time.Time) DateExpression {
	dateLiteral := &dateLiteral{}
	dateLiteral.literalExpressionImpl = *literal(t)
	dateLiteral.dateInterfaceImpl.parent = dateLiteral

	return dateLiteral
}

func formatNanoseconds(nanoseconds ...time.Duration) string {
	if len(nanoseconds) > 0 && nanoseconds[0] != 0 {
		duration := fmt.Sprintf("%09d", nanoseconds[0])
		i := len(duration) - 1
		for ; i >= 3; i-- {
			if duration[i] != '0' {
				break
			}
		}

		return "." + duration[0:i+1]
	}

	return ""
}

//--------------------------------------------------//
type nullLiteral struct {
	expressionInterfaceImpl
}

func newNullLiteral() Expression {
	nullExpression := &nullLiteral{}

	nullExpression.expressionInterfaceImpl.Parent = nullExpression

	return nullExpression
}

func (n *nullLiteral) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("NULL")
}

//--------------------------------------------------//
type starLiteral struct {
	expressionInterfaceImpl
}

func newStarLiteral() Expression {
	starExpression := &starLiteral{}

	starExpression.expressionInterfaceImpl.Parent = starExpression

	return starExpression
}

func (n *starLiteral) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("*")
}

//---------------------------------------------------//

type wrap struct {
	expressionInterfaceImpl
	expressions []Expression
}

func (n *wrap) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("(")
	serializeExpressionList(statement, n.expressions, ", ", out)
	out.WriteString(")")
}

// WRAP wraps list of expressions with brackets '(' and ')'
func WRAP(expression ...Expression) Expression {
	wrap := &wrap{expressions: expression}
	wrap.expressionInterfaceImpl.Parent = wrap

	return wrap
}

//---------------------------------------------------//

type rawExpression struct {
	expressionInterfaceImpl

	raw string
}

func (n *rawExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString(n.raw)
}

// Raw can be used for any unsupported functions, operators or expressions.
// For example: Raw("current_database()")
func Raw(raw string) Expression {
	rawExp := &rawExpression{raw: raw}
	rawExp.expressionInterfaceImpl.Parent = rawExp

	return rawExp
}
