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

func constLiteral(value interface{}) *literalExpressionImpl {
	exp := literal(value)
	exp.constant = true

	return exp
}

func (l *literalExpressionImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	if l.constant {
		out.insertConstantArgument(l.value)
	} else {
		out.insertParametrizedArgument(l.value)
	}

	return nil
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

// Int is constructor for integer expressions literals.
func Int(value int64, constant ...bool) IntegerExpression {
	numLiteral := &integerLiteralExpression{}

	numLiteral.literalExpressionImpl = *literal(value)
	if len(constant) > 0 && constant[0] == true {
		numLiteral.constant = true
	}

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

// Float creates new float literal expression
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
func String(value string, constant ...bool) StringExpression {
	stringLiteral := stringLiteral{}
	stringLiteral.literalExpressionImpl = *literal(value)
	if len(constant) > 0 && constant[0] == true {
		stringLiteral.constant = true
	}

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

// Timestamp creates new timestamp literal expression
func Timestampz(year int, month time.Month, day, hour, minute, second int, nanoseconds time.Duration, timezone string) TimestampzExpression {
	timestamp := &timestampzLiteral{}
	timeStr := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	timeStr += formatNanoseconds(nanoseconds)
	timeStr += " " + timezone

	timestamp.literalExpressionImpl = *literal(timeStr)
	timestamp.timestampzInterfaceImpl.parent = timestamp
	return timestamp
}

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

//Date creates new date expression
func Date(year int, month time.Month, day int) DateExpression {
	dateLiteral := &dateLiteral{}

	timeStr := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	dateLiteral.literalExpressionImpl = *literal(timeStr)
	dateLiteral.dateInterfaceImpl.parent = dateLiteral

	return dateLiteral
}

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
	ExpressionInterfaceImpl
}

func newNullLiteral() Expression {
	nullExpression := &nullLiteral{}

	nullExpression.ExpressionInterfaceImpl.Parent = nullExpression

	return nullExpression
}

func (n *nullLiteral) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	out.WriteString("NULL")
	return nil
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

func (n *starLiteral) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	out.WriteString("*")
	return nil
}

//---------------------------------------------------//

type wrap struct {
	ExpressionInterfaceImpl
	expressions []Expression
}

func (n *wrap) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	out.WriteString("(")
	err := serializeExpressionList(statement, n.expressions, ", ", out)
	out.WriteString(")")
	return err
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

	raw string
}

func (n *rawExpression) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	out.WriteString(n.raw)
	return nil
}

// Raw can be used for any unsupported functions, operators or expressions.
// For example: Raw("current_database()")
func Raw(raw string) Expression {
	rawExp := &rawExpression{raw: raw}
	rawExp.ExpressionInterfaceImpl.Parent = rawExp

	return rawExp
}
