package jet

import "fmt"

// Expression is a common interface for all expressions.
// Can be Bool, Int, Float, String, Date, Time, Timez, Timestamp or Timestampz expressions.
type Expression interface {
	Serializer
	Projection
	GroupByClause
	OrderByClause
	expressionOrColumnList

	serializeForJsonValue(statement StatementType, out *SQLBuilder)
	setRoot(root Expression)

	// IS_NULL tests expression whether it is a NULL value.
	IS_NULL() BoolExpression
	// IS_NOT_NULL tests expression whether it is a non-NULL value.
	IS_NOT_NULL() BoolExpression

	// IN checks if this expressions matches any in expressions list
	IN(expressions ...Expression) BoolExpression
	// NOT_IN checks if this expressions is different of all expressions in expressions list
	NOT_IN(expressions ...Expression) BoolExpression

	// AS the temporary alias name to assign to the expression
	AS(alias string) Projection

	// ASC expression will be used to sort query result in ascending order
	ASC() OrderByClause
	// DESC expression will be used to sort query result in descending order
	DESC() OrderByClause
}

// ExpressionInterfaceImpl implements Expression interface methods
type ExpressionInterfaceImpl struct {
	Root Expression
}

func (e *ExpressionInterfaceImpl) isExpressionOrColumnList() {}

func (e *ExpressionInterfaceImpl) setRoot(root Expression) {
	e.Root = root
}

func (e *ExpressionInterfaceImpl) fromImpl(subQuery SelectTable) Projection {
	panic(fmt.Sprintf("jet: can't export unaliased expression subQuery: %s, expression: %s",
		subQuery.Alias(), serializeToDefaultDebugString(e.Root)))
}

// IS_NULL tests expression whether it is a NULL value.
func (e *ExpressionInterfaceImpl) IS_NULL() BoolExpression {
	return newPostfixBoolOperatorExpression(e.Root, "IS NULL")
}

// IS_NOT_NULL tests expression whether it is a non-NULL value.
func (e *ExpressionInterfaceImpl) IS_NOT_NULL() BoolExpression {
	return newPostfixBoolOperatorExpression(e.Root, "IS NOT NULL")
}

// IN checks if this expressions matches any in expressions list
func (e *ExpressionInterfaceImpl) IN(expressions ...Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(e.Root, wrap(expressions...), "IN")
}

// NOT_IN checks if this expressions is different of all expressions in expressions list
func (e *ExpressionInterfaceImpl) NOT_IN(expressions ...Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(e.Root, wrap(expressions...), "NOT IN")
}

// AS the temporary alias name to assign to the expression
func (e *ExpressionInterfaceImpl) AS(alias string) Projection {
	return newAlias(e.Root, alias)
}

// ASC expression will be used to sort a query result in ascending order
func (e *ExpressionInterfaceImpl) ASC() OrderByClause {
	return newOrderByAscending(e.Root, true)
}

// DESC expression will be used to sort a query result in descending order
func (e *ExpressionInterfaceImpl) DESC() OrderByClause {
	return newOrderByAscending(e.Root, false)
}

// NULLS_FIRST specifies sort where null values appear before all non-null values
func (e *ExpressionInterfaceImpl) NULLS_FIRST() OrderByClause {
	return newOrderByNullsFirst(e.Root, true)
}

// NULLS_LAST specifies sort where null values appear after all non-null values
func (e *ExpressionInterfaceImpl) NULLS_LAST() OrderByClause {
	return newOrderByNullsFirst(e.Root, false)
}

func (e *ExpressionInterfaceImpl) serializeForGroupBy(statement StatementType, out *SQLBuilder) {
	e.Root.serialize(statement, out, NoWrap)
}

func (e *ExpressionInterfaceImpl) serializeForProjection(statement StatementType, out *SQLBuilder) {
	e.Root.serialize(statement, out, NoWrap)
}

func (e *ExpressionInterfaceImpl) serializeForJsonObjEntry(statement StatementType, out *SQLBuilder) {
	panic("jet: expression need to be aliased when used as SELECT JSON projection.")
}

func (e *ExpressionInterfaceImpl) serializeForRowToJsonProjection(statement StatementType, out *SQLBuilder) {
	panic("jet: expression need to be aliased when used as SELECT JSON projection.")
}

func (e *ExpressionInterfaceImpl) serializeForJsonValue(statement StatementType, out *SQLBuilder) {
	out.Dialect.JsonValueEncode(e.Root).serialize(statement, out)
}

func (e *ExpressionInterfaceImpl) serializeForOrderBy(statement StatementType, out *SQLBuilder) {
	e.Root.serialize(statement, out, NoWrap)
}

type expression struct {
	ExpressionInterfaceImpl
	Serializer
}

func newExpression(serializer Serializer) Expression {
	expr := &expression{
		Serializer: serializer,
	}

	expr.ExpressionInterfaceImpl.Root = expr

	return expr
}

// Representation of binary operations (e.g. comparisons, arithmetic)
type binaryOperatorSerializer struct {
	lhs, rhs        Serializer
	additionalParam Serializer
	operator        string
}

func (c *binaryOperatorSerializer) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if serializeOverride := out.Dialect.OperatorSerializeOverride(c.operator); serializeOverride != nil {
		serializeOverrideFunc := serializeOverride(c.lhs, c.rhs, c.additionalParam)
		serializeOverrideFunc(statement, out, FallTrough(options)...)
	} else {
		c.lhs.serialize(statement, out, FallTrough(options)...)
		out.WriteString(c.operator)
		c.rhs.serialize(statement, out, FallTrough(options)...)
	}
}

// NewBinaryOperatorExpression creates new binaryOperatorExpression
func NewBinaryOperatorExpression(lhs, rhs Serializer, operator string, additionalParam ...Expression) Expression {
	return newExpression(optionalWrap(&binaryOperatorSerializer{
		lhs:             lhs,
		rhs:             rhs,
		additionalParam: OptionalOrDefault(additionalParam, nil),
		operator:        operator,
	}))
}

type serializersWithOperator struct {
	operator    string
	serializers []Serializer
}

func (s *serializersWithOperator) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if len(s.serializers) == 0 {
		panic("jet: syntax error, expression list empty")
	}

	shouldWrap := len(s.serializers) > 1
	if shouldWrap {
		out.WriteByte('(')
		out.IncreaseIdent(tabSize)
		out.NewLine()
	}

	for i, expression := range s.serializers {
		if i == 1 {
			out.IncreaseIdent(tabSize)
		}
		if i > 0 {
			out.NewLine()
			out.WriteString(s.operator)
		}

		out.IncreaseIdent(len(s.operator) + 1)
		expression.serialize(statement, out, FallTrough(options)...)
		out.DecreaseIdent(len(s.operator) + 1)
	}

	if len(s.serializers) > 1 {
		out.DecreaseIdent(tabSize)
	}

	if shouldWrap {
		out.DecreaseIdent(tabSize)
		out.NewLine()
		out.WriteByte(')')
	}
}

func newBoolExpressionListOperator(operator string, expressions []BoolExpression) BoolExpression {
	return BoolExp(newExpression(&serializersWithOperator{
		operator:    operator,
		serializers: ToSerializerList(expressions),
	}))
}

func newPrefixOperatorExpression(expression Expression, operator string) Expression {
	return CustomExpression(Token(operator), expression)
}

func newPostfixOperatorExpression(expression Expression, operator string) Expression {
	return CustomExpression(expression, Token(operator))
}

type betweenOperatorSerializer struct {
	expression Expression
	notBetween bool
	min        Expression
	max        Expression
}

func (b *betweenOperatorSerializer) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	b.expression.serialize(statement, out, FallTrough(options)...)
	if b.notBetween {
		out.WriteString("NOT")
	}
	out.WriteString("BETWEEN")
	b.min.serialize(statement, out, FallTrough(options)...)
	out.WriteString("AND")
	b.max.serialize(statement, out, FallTrough(options)...)
}

// NewBetweenOperatorExpression creates new BETWEEN operator expression
func NewBetweenOperatorExpression(expression, min, max Expression, notBetween bool) BoolExpression {
	return BoolExp(newExpression(
		optionalWrap(&betweenOperatorSerializer{
			expression: expression,
			notBetween: notBetween,
			min:        min,
			max:        max,
		}),
	))
}
