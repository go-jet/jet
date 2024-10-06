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
	ExpressionInterfaceImpl

	value    interface{}
	constant bool
}

func literal(value interface{}, optionalConstant ...bool) *literalExpressionImpl {
	exp := literalExpressionImpl{value: value}

	if len(optionalConstant) > 0 {
		exp.constant = optionalConstant[0]
	}

	exp.ExpressionInterfaceImpl.Parent = &exp

	return &exp
}

// Literal is injected directly to SQL query, and does not appear in parametrized argument list.
func Literal(value interface{}) *literalExpressionImpl {
	exp := literal(value)
	return exp
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

func intLiteral(value interface{}) IntegerExpression {
	numLiteral := &integerLiteralExpression{}

	numLiteral.literalExpressionImpl = *literal(value)

	numLiteral.literalExpressionImpl.Parent = numLiteral
	numLiteral.integerInterfaceImpl.parent = numLiteral

	return numLiteral
}

// Int creates a new 64 bit signed integer literal
func Int(value int64) IntegerExpression {
	return intLiteral(value)
}

// Int8 creates a new 8 bit signed integer literal
func Int8(value int8) IntegerExpression {
	return intLiteral(value)
}

// Int16 creates a new 16 bit signed integer literal
func Int16(value int16) IntegerExpression {
	return intLiteral(value)
}

// Int32 creates a new 32 bit signed integer literal
func Int32(value int32) IntegerExpression {
	return intLiteral(value)
}

// Uint8 creates a new 8 bit unsigned integer literal
func Uint8(value uint8) IntegerExpression {
	return intLiteral(value)
}

// Uint16 creates a new 16 bit unsigned integer literal
func Uint16(value uint16) IntegerExpression {
	return intLiteral(value)
}

// Uint32 creates a new 32 bit unsigned integer literal
func Uint32(value uint32) IntegerExpression {
	return intLiteral(value)
}

// Uint64 creates a new 64 bit unsigned integer literal
func Uint64(value uint64) IntegerExpression {
	return intLiteral(value)
}

// ---------------------------------------------------//
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

// ---------------------------------------------------//
type floatLiteral struct {
	floatInterfaceImpl
	literalExpressionImpl
}

// Float creates new float literal from float64 value
func Float(value float64) FloatExpression {
	floatLiteral := floatLiteral{}
	floatLiteral.literalExpressionImpl = *literal(value)

	floatLiteral.floatInterfaceImpl.parent = &floatLiteral

	return &floatLiteral
}

// Decimal creates new float literal from string value
func Decimal(value string) FloatExpression {
	floatLiteral := floatLiteral{}
	floatLiteral.literalExpressionImpl = *literal(value)

	floatLiteral.floatInterfaceImpl.parent = &floatLiteral

	return &floatLiteral
}

// ---------------------------------------------------//
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

var (
	// NULL is jet equivalent of SQL NULL
	NULL = newNullLiteral()
	// STAR is jet equivalent of SQL *
	STAR = newStarLiteral()
	// PLUS_INFINITY is jet equivalent for sql infinity
	PLUS_INFINITY = String("infinity")
	// MINUS_INFINITY is jet equivalent for sql -infinity
	MINUS_INFINITY = String("-infinity")
)

type nullLiteral struct {
	ExpressionInterfaceImpl
}

func newNullLiteral() Expression {
	nullExpression := &nullLiteral{}

	nullExpression.ExpressionInterfaceImpl.Parent = nullExpression

	return nullExpression
}

func (n *nullLiteral) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("NULL")
}

// --------------------------------------------------//
type starLiteral struct {
	ExpressionInterfaceImpl
}

func newStarLiteral() Expression {
	starExpression := &starLiteral{}

	starExpression.ExpressionInterfaceImpl.Parent = starExpression

	return starExpression
}

func (n *starLiteral) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("*")
}

//---------------------------------------------------//

type rawExpression struct {
	ExpressionInterfaceImpl

	Raw           string
	NamedArgument map[string]interface{}
	noWrap        bool
}

func (n *rawExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if !n.noWrap && !contains(options, NoWrap) {
		out.WriteByte('(')
	}

	out.insertRawQuery(n.Raw, n.NamedArgument)

	if !n.noWrap && !contains(options, NoWrap) {
		out.WriteByte(')')
	}
}

// Raw can be used for any unsupported functions, operators or expressions.
// For example: Raw("current_database()")
func Raw(raw string, namedArgs ...map[string]interface{}) Expression {
	var namedArguments map[string]interface{}

	if len(namedArgs) > 0 {
		namedArguments = namedArgs[0]
	}

	rawExp := &rawExpression{
		Raw:           raw,
		NamedArgument: namedArguments,
	}
	rawExp.ExpressionInterfaceImpl.Parent = rawExp

	return rawExp
}

// RawWithParent is a Raw constructor used for construction dialect specific expression
func RawWithParent(raw string, parent ...Expression) Expression {
	rawExp := &rawExpression{
		Raw:    raw,
		noWrap: true,
	}
	rawExp.ExpressionInterfaceImpl.Parent = OptionalOrDefaultExpression(rawExp, parent...)

	return rawExp
}

// RawBool helper that for raw string boolean expressions
func RawBool(raw string, namedArgs ...map[string]interface{}) BoolExpression {
	return BoolExp(Raw(raw, namedArgs...))
}

// RawInt helper that for integer expressions
func RawInt(raw string, namedArgs ...map[string]interface{}) IntegerExpression {
	return IntExp(Raw(raw, namedArgs...))
}

// RawFloat helper that for float expressions
func RawFloat(raw string, namedArgs ...map[string]interface{}) FloatExpression {
	return FloatExp(Raw(raw, namedArgs...))
}

// RawString helper that for string expressions
func RawString(raw string, namedArgs ...map[string]interface{}) StringExpression {
	return StringExp(Raw(raw, namedArgs...))
}

// RawTime helper that for time expressions
func RawTime(raw string, namedArgs ...map[string]interface{}) TimeExpression {
	return TimeExp(Raw(raw, namedArgs...))
}

// RawTimez helper that for time with time zone expressions
func RawTimez(raw string, namedArgs ...map[string]interface{}) TimezExpression {
	return TimezExp(Raw(raw, namedArgs...))
}

// RawTimestamp helper that for timestamp expressions
func RawTimestamp(raw string, namedArgs ...map[string]interface{}) TimestampExpression {
	return TimestampExp(Raw(raw, namedArgs...))
}

// RawTimestampz helper that for timestamp with time zone expressions
func RawTimestampz(raw string, namedArgs ...map[string]interface{}) TimestampzExpression {
	return TimestampzExp(Raw(raw, namedArgs...))
}

// RawDate helper that for date expressions
func RawDate(raw string, namedArgs ...map[string]interface{}) DateExpression {
	return DateExp(Raw(raw, namedArgs...))
}

// RawRange helper that for range expressions
func RawRange[T Expression](raw string, namedArgs ...map[string]interface{}) Range[T] {
	return RangeExp[T](Raw(raw, namedArgs...))
}

// UUID is a helper function to create string literal expression from uuid object
// value can be any uuid type with a String method
func UUID(value fmt.Stringer) StringExpression {
	return String(value.String())
}
