package jet

import (
	"fmt"
	"strconv"
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

var (
	// NULL is jet equivalent of SQL NULL
	NULL = newNullLiteral()
	// STAR is jet equivalent of SQL *
	STAR = newStarLiteral()
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

//--------------------------------------------------//
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

type wrap struct {
	ExpressionInterfaceImpl
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
	wrap.ExpressionInterfaceImpl.Parent = wrap

	return wrap
}

//---------------------------------------------------//

type rawExpression struct {
	ExpressionInterfaceImpl

	Raw string
}

func (n *rawExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString(n.Raw)
}

// Raw can be used for any unsupported functions, operators or expressions.
// For example: Raw("current_database()")
func Raw(raw string, parent ...Expression) Expression {
	rawExp := &rawExpression{Raw: raw}
	rawExp.ExpressionInterfaceImpl.Parent = OptionalOrDefaultExpression(rawExp, parent...)

	return rawExp
}

//---------------------------------------------------//

type rawParameterizedExpression struct {
	ExpressionInterfaceImpl

	parameters []Expression
	Raw string
}

func (n *rawParameterizedExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	paramsLen := len(n.parameters)
	end := len(n.Raw)
	for i := 0; i < end; {
		lasti := i
		for i < end && n.Raw[i] != '$' {
			i++
		}
		if i > lasti {
			out.WriteString(n.Raw[lasti:i])
		}
		if i >= end {
			// done processing raw string
			break
		}

		// Process one parameter
		i++

		// Extract the index
		startIdx := i
		endIdx := i
		for ; i < end; i++ {
			endIdx = i
			if n.Raw[i] < '0' || n.Raw[i] > '9' {
				break
			}
		}

		if i >= end {
			endIdx++
		}

		// Convert the index
		if startIdx == endIdx {
			panic("jet: raw expression cannot contain $")
		}

		idx, err := strconv.Atoi(n.Raw[startIdx:endIdx])
		if err != nil {
			panic("jet: unable to convert $ index to integer")
		}

		// Serialize the parameter
		if idx > paramsLen {
			panic("jet: index of $ was not found in list of parameters")
		}

		n.parameters[idx-1].serialize(statement, out, options...)
	}

}

// Raw can be used for any unsupported functions, operators or expressions that require parameters.
// For example: RawP("my_function($1)", String("my_parameter"))
func RawP(raw string, parameters ...Expression) Expression {
	rawParamsExp := &rawParameterizedExpression{Raw: raw, parameters: parameters}
	rawParamsExp.ExpressionInterfaceImpl.Parent = rawParamsExp

	return rawParamsExp
}
