package jet

import (
	"fmt"
	"time"
)

type literalSerializer struct {
	value    interface{}
	constant bool
}

func (l *literalSerializer) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if l.constant {
		out.insertConstantArgument(l.value)
	} else {
		out.insertParametrizedArgument(l.value)
	}
}

// Literal is injected directly to SQL query, and does not appear in parametrized argument list.
func Literal(value interface{}) Expression {
	return newExpression(&literalSerializer{
		value:    value,
		constant: false,
	})
}

// FixedLiteral is injected directly to SQL query, and does not appear in parametrized argument list.
func FixedLiteral(value interface{}) Expression {
	return newExpression(&literalSerializer{
		value:    value,
		constant: true,
	})
}

// Int creates a new 64 bit signed integer literal
func Int(value int64) IntegerExpression {
	return IntExp(Literal(value))
}

// Int8 creates a new 8 bit signed integer literal
func Int8(value int8) IntegerExpression {
	return IntExp(Literal(value))
}

// Int16 creates a new 16 bit signed integer literal
func Int16(value int16) IntegerExpression {
	return IntExp(Literal(value))
}

// Int32 creates a new 32 bit signed integer literal
func Int32(value int32) IntegerExpression {
	return IntExp(Literal(value))
}

// Uint8 creates a new 8 bit unsigned integer literal
func Uint8(value uint8) IntegerExpression {
	return IntExp(Literal(value))
}

// Uint16 creates a new 16 bit unsigned integer literal
func Uint16(value uint16) IntegerExpression {
	return IntExp(Literal(value))
}

// Uint32 creates a new 32 bit unsigned integer literal
func Uint32(value uint32) IntegerExpression {
	return IntExp(Literal(value))
}

// Uint64 creates a new 64 bit unsigned integer literal
func Uint64(value uint64) IntegerExpression {
	return IntExp(Literal(value))
}

// Bool creates new bool literal expression
func Bool(value bool) BoolExpression {
	return BoolExp(Literal(value))
}

// Float creates new float literal from float64 value
func Float(value float64) FloatExpression {
	return FloatExp(Literal(value))
}

// Decimal creates new float literal from string value
func Decimal(value string) FloatExpression {
	return FloatExp(Literal(value))
}

// String creates new string literal expression
func String(value string) StringExpression {
	return StringExp(Literal(value))
}

// Time creates new time literal expression
func Time(hour, minute, second int, nanoseconds ...time.Duration) TimeExpression {
	timeStr := fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
	timeStr += formatNanoseconds(nanoseconds...)

	return TimeExp(Literal(timeStr))
}

// TimeT creates new time literal expression from time.Time object
func TimeT(t time.Time) TimeExpression {
	return TimeExp(Literal(t))
}

// Timez creates new time with time zone literal expression
func Timez(hour, minute, second int, nanoseconds time.Duration, timezone string) TimezExpression {
	timeStr := fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
	timeStr += formatNanoseconds(nanoseconds)
	timeStr += " " + timezone

	return TimezExp(Literal(timeStr))
}

// TimezT creates new time with time zone literal expression from time.Time object
func TimezT(t time.Time) TimezExpression {
	return TimezExp(Literal(t))
}

// Timestamp creates new timestamp literal expression
func Timestamp(year int, month time.Month, day, hour, minute, second int, nanoseconds ...time.Duration) TimestampExpression {
	timeStr := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	timeStr += formatNanoseconds(nanoseconds...)

	return TimestampExp(Literal(timeStr))
}

// TimestampT creates new timestamp literal expression from time.Time object
func TimestampT(t time.Time) TimestampExpression {
	return TimestampExp(Literal(t))
}

// Timestampz creates new timestamp with time zone literal expression
func Timestampz(year int, month time.Month, day, hour, minute, second int, nanoseconds time.Duration, timezone string) TimestampzExpression {
	timeStr := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	timeStr += formatNanoseconds(nanoseconds)
	timeStr += " " + timezone

	return TimestampzExp(Literal(timeStr))
}

// TimestampzT creates new timestamp literal expression from time.Time object
func TimestampzT(t time.Time) TimestampzExpression {
	return TimestampzExp(Literal(t))
}

// Date creates new date literal expression
func Date(year int, month time.Month, day int) DateExpression {
	timeStr := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	return DateExp(Literal(timeStr))
}

// DateT creates new date literal expression from time.Time object
func DateT(t time.Time) DateExpression {
	return DateExp(Literal(t))
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
	NULL = newExpression(Keyword("NULL"))
	// STAR is jet equivalent of SQL *
	STAR = newExpression(Keyword("*"))
	// PLUS_INFINITY is jet equivalent for sql infinity
	PLUS_INFINITY = String("infinity")
	// MINUS_INFINITY is jet equivalent for sql -infinity
	MINUS_INFINITY = String("-infinity")
)

//---------------------------------------------------//

type rawSerializer struct {
	Raw           string
	NamedArgument map[string]interface{}
}

func (n *rawSerializer) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	optionalWrap(out, options, func(out *SQLBuilder, options []SerializeOption) {
		out.insertRawQuery(n.Raw, n.NamedArgument)
	})
}

// Raw can be used for any unsupported functions, operators or expressions.
// For example: Raw("current_database()")
func Raw(raw string, namedArgs ...map[string]interface{}) Expression {
	return newExpression(&rawSerializer{
		Raw:           raw,
		NamedArgument: singleOptional(namedArgs),
	})
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

// RawBlob is raw query helper that for blob expressions
func RawBlob(raw string, namedArgs ...map[string]interface{}) BlobExpression {
	return BlobExp(Raw(raw, namedArgs...))
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
