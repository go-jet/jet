package jet

import "fmt"

// Representation of an escaped literal
type literalExpression struct {
	expressionInterfaceImpl
	noOpVisitorImpl

	value    interface{}
	constant bool
}

func literal(value interface{}, optionalConstant ...bool) *literalExpression {
	exp := literalExpression{value: value}

	if len(optionalConstant) > 0 {
		exp.constant = optionalConstant[0]
	}

	exp.expressionInterfaceImpl.parent = &exp

	return &exp
}

func constLiteral(value interface{}) *literalExpression {
	exp := literal(value)
	exp.constant = true

	return exp
}

func (l literalExpression) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	if l.constant {
		out.insertConstantArgument(l.value)
	} else {
		out.insertParametrizedArgument(l.value)
	}

	return nil
}

// Int is constructor for integer expressions literals.
func Int(value int64, constant ...bool) IntegerExpression {
	return IntExp(literal(value, constant...))
}

// Bool creates new bool literal expression
func Bool(value bool) BoolExpression {
	return BoolExp(literal(value))
}

// Float creates new float literal expression
func Float(value float64) FloatExpression {
	return FloatExp(literal(value))
}

// String creates new string literal expression
func String(value string) StringExpression {
	return StringExp(literal(value))
}

// Time creates new time literal expression
func Time(hour, minute, second, milliseconds int) TimeExpression {
	timeStr := fmt.Sprintf("%02d:%02d:%02d.%03d", hour, minute, second, milliseconds)

	return TimeExp(literal(timeStr))
}

// Timez creates new time with time zone literal expression
func Timez(hour, minute, second, milliseconds, timezone int) TimezExpression {
	timeStr := fmt.Sprintf("%02d:%02d:%02d.%03d %+03d", hour, minute, second, milliseconds, timezone)

	return TimezExp(literal(timeStr))
}

// Timestamp creates new timestamp literal expression
func Timestamp(year, month, day, hour, minute, second, milliseconds int) TimestampExpression {
	timeStr := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%03d", year, month, day, hour, minute, second, milliseconds)

	return TimestampExp(literal(timeStr))
}

// Timestampz creates new timestamp with time zone literal expression
func Timestampz(year, month, day, hour, minute, second, milliseconds, timezone int) TimestampzExpression {
	timeStr := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%03d %+04d",
		year, month, day, hour, minute, second, milliseconds, timezone)

	return TimestampzExp(literal(timeStr))
}

//Date creates new date expression
func Date(year, month, day int) DateExpression {
	timeStr := fmt.Sprintf("%04d-%02d-%02d", year, month, day)

	return DateExp(literal(timeStr))
}

//--------------------------------------------------//
type nullLiteral struct {
	expressionInterfaceImpl
	noOpVisitorImpl
}

func newNullLiteral() Expression {
	nullExpression := &nullLiteral{}

	nullExpression.expressionInterfaceImpl.parent = nullExpression

	return nullExpression
}

func (n *nullLiteral) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	out.WriteString("NULL")
	return nil
}

//--------------------------------------------------//
type starLiteral struct {
	expressionInterfaceImpl
	noOpVisitorImpl
}

func newStarLiteral() Expression {
	starExpression := &starLiteral{}

	starExpression.expressionInterfaceImpl.parent = starExpression

	return starExpression
}

func (n *starLiteral) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	out.WriteString("*")
	return nil
}

//---------------------------------------------------//

type wrap struct {
	expressionInterfaceImpl
	expressions []Expression
}

func (n *wrap) accept(visitor visitor) {
	for _, exp := range n.expressions {
		exp.accept(visitor)
	}
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
	wrap.expressionInterfaceImpl.parent = wrap

	return wrap
}

//---------------------------------------------------//

type rawExpression struct {
	expressionInterfaceImpl
	noOpVisitorImpl

	raw string
}

func (n *rawExpression) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	out.WriteString(n.raw)
	return nil
}

// RAW can be used for any unsupported functions, operators or expressions.
// For example: RAW("current_database()")
func RAW(raw string) Expression {
	rawExp := &rawExpression{raw: raw}
	rawExp.expressionInterfaceImpl.parent = rawExp

	return rawExp
}
