package jet

import "fmt"

// Expression is common interface for all expressions.
// Can be Bool, Int, Float, String, Date, Time, Timez, Timestamp or Timestampz expressions.
type Expression interface {
	Serializer
	Projection
	GroupByClause
	OrderByClause

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
	Parent Expression
}

func (e *ExpressionInterfaceImpl) fromImpl(subQuery SelectTable) Projection {
	panic(fmt.Sprintf("jet: can't export unaliased expression subQuery: %s, expression: %s",
		subQuery.Alias(), serializeToDefaultDebugString(e.Parent)))
}

// IS_NULL tests expression whether it is a NULL value.
func (e *ExpressionInterfaceImpl) IS_NULL() BoolExpression {
	return newPostfixBoolOperatorExpression(e.Parent, "IS NULL")
}

// IS_NOT_NULL tests expression whether it is a non-NULL value.
func (e *ExpressionInterfaceImpl) IS_NOT_NULL() BoolExpression {
	return newPostfixBoolOperatorExpression(e.Parent, "IS NOT NULL")
}

// IN checks if this expressions matches any in expressions list
func (e *ExpressionInterfaceImpl) IN(expressions ...Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(e.Parent, wrap(expressions...), "IN")
}

// NOT_IN checks if this expressions is different of all expressions in expressions list
func (e *ExpressionInterfaceImpl) NOT_IN(expressions ...Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(e.Parent, wrap(expressions...), "NOT IN")
}

// AS the temporary alias name to assign to the expression
func (e *ExpressionInterfaceImpl) AS(alias string) Projection {
	return newAlias(e.Parent, alias)
}

// ASC expression will be used to sort a query result in ascending order
func (e *ExpressionInterfaceImpl) ASC() OrderByClause {
	return newOrderByAscending(e.Parent, true)
}

// DESC expression will be used to sort a query result in descending order
func (e *ExpressionInterfaceImpl) DESC() OrderByClause {
	return newOrderByAscending(e.Parent, false)
}

// NULLS_FIRST specifies sort where null values appear before all non-null values
func (e *ExpressionInterfaceImpl) NULLS_FIRST() OrderByClause {
	return newOrderByNullsFirst(e.Parent, true)
}

// NULLS_LAST specifies sort where null values appear after all non-null values
func (e *ExpressionInterfaceImpl) NULLS_LAST() OrderByClause {
	return newOrderByNullsFirst(e.Parent, false)
}

func (e *ExpressionInterfaceImpl) serializeForGroupBy(statement StatementType, out *SQLBuilder) {
	e.Parent.serialize(statement, out, NoWrap)
}

func (e *ExpressionInterfaceImpl) serializeForProjection(statement StatementType, out *SQLBuilder) {
	e.Parent.serialize(statement, out, NoWrap)
}

func (e *ExpressionInterfaceImpl) serializeForOrderBy(statement StatementType, out *SQLBuilder) {
	e.Parent.serialize(statement, out, NoWrap)
}

// Representation of binary operations (e.g. comparisons, arithmetic)
type binaryOperatorExpression struct {
	ExpressionInterfaceImpl

	lhs, rhs        Serializer
	additionalParam Serializer
	operator        string
}

// NewBinaryOperatorExpression creates new binaryOperatorExpression
func NewBinaryOperatorExpression(lhs, rhs Serializer, operator string, additionalParam ...Expression) Expression {
	binaryExpression := &binaryOperatorExpression{
		lhs:      lhs,
		rhs:      rhs,
		operator: operator,
	}

	if len(additionalParam) > 0 {
		binaryExpression.additionalParam = additionalParam[0]
	}

	binaryExpression.ExpressionInterfaceImpl.Parent = binaryExpression

	return complexExpr(binaryExpression)
}

func (c *binaryOperatorExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if serializeOverride := out.Dialect.OperatorSerializeOverride(c.operator); serializeOverride != nil {
		serializeOverrideFunc := serializeOverride(c.lhs, c.rhs, c.additionalParam)
		serializeOverrideFunc(statement, out, FallTrough(options)...)
	} else {
		c.lhs.serialize(statement, out, FallTrough(options)...)
		out.WriteString(c.operator)
		c.rhs.serialize(statement, out, FallTrough(options)...)
	}
}

type expressionListOperator struct {
	ExpressionInterfaceImpl

	operator    string
	expressions []Expression
}

func newExpressionListOperator(operator string, expressions ...Expression) *expressionListOperator {
	ret := &expressionListOperator{
		operator:    operator,
		expressions: expressions,
	}

	ret.ExpressionInterfaceImpl.Parent = ret

	return ret
}

func newBoolExpressionListOperator(operator string, expressions ...BoolExpression) BoolExpression {
	return BoolExp(newExpressionListOperator(operator, BoolExpressionListToExpressionList(expressions)...))
}

func (elo *expressionListOperator) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if len(elo.expressions) == 0 {
		panic("jet: syntax error, expression list empty")
	}

	shouldWrap := len(elo.expressions) > 1
	if shouldWrap {
		out.WriteByte('(')
		out.IncreaseIdent(tabSize)
		out.NewLine()
	}

	for i, expression := range elo.expressions {
		if i == 1 {
			out.IncreaseIdent(tabSize)
		}
		if i > 0 {
			out.NewLine()
			out.WriteString(elo.operator)
		}

		out.IncreaseIdent(len(elo.operator) + 1)
		expression.serialize(statement, out, FallTrough(options)...)
		out.DecreaseIdent(len(elo.operator) + 1)
	}

	if len(elo.expressions) > 1 {
		out.DecreaseIdent(tabSize)
	}

	if shouldWrap {
		out.DecreaseIdent(tabSize)
		out.NewLine()
		out.WriteByte(')')
	}
}

// A prefix operator Expression
type prefixExpression struct {
	ExpressionInterfaceImpl

	expression Expression
	operator   string
}

func newPrefixOperatorExpression(expression Expression, operator string) Expression {
	prefixExpression := &prefixExpression{
		expression: expression,
		operator:   operator,
	}
	prefixExpression.ExpressionInterfaceImpl.Parent = prefixExpression

	return complexExpr(prefixExpression)
}

func (p *prefixExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString(p.operator)
	p.expression.serialize(statement, out, FallTrough(options)...)
}

// A postfix operator Expression
type postfixOpExpression struct {
	ExpressionInterfaceImpl

	expression Expression
	operator   string
}

func newPostfixOperatorExpression(expression Expression, operator string) *postfixOpExpression {
	postfixOpExpression := &postfixOpExpression{
		expression: expression,
		operator:   operator,
	}

	postfixOpExpression.ExpressionInterfaceImpl.Parent = postfixOpExpression

	return postfixOpExpression
}

func (p *postfixOpExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	p.expression.serialize(statement, out, FallTrough(options)...)
	out.WriteString(p.operator)
}

type betweenOperatorExpression struct {
	ExpressionInterfaceImpl

	expression Expression
	notBetween bool
	min        Expression
	max        Expression
}

// NewBetweenOperatorExpression creates new BETWEEN operator expression
func NewBetweenOperatorExpression(expression, min, max Expression, notBetween bool) BoolExpression {
	newBetweenOperator := &betweenOperatorExpression{
		expression: expression,
		notBetween: notBetween,
		min:        min,
		max:        max,
	}

	newBetweenOperator.ExpressionInterfaceImpl.Parent = newBetweenOperator

	return BoolExp(complexExpr(newBetweenOperator))
}

func (p *betweenOperatorExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	p.expression.serialize(statement, out, FallTrough(options)...)
	if p.notBetween {
		out.WriteString("NOT")
	}
	out.WriteString("BETWEEN")
	p.min.serialize(statement, out, FallTrough(options)...)
	out.WriteString("AND")
	p.max.serialize(statement, out, FallTrough(options)...)
}

type customExpression struct {
	ExpressionInterfaceImpl
	parts []Serializer
}

func CustomExpression(parts ...Serializer) Expression {
	ret := customExpression{
		parts: parts,
	}
	ret.ExpressionInterfaceImpl.Parent = &ret
	return &ret
}

func (c *customExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	for _, expression := range c.parts {
		expression.serialize(statement, out, options...)
	}
}

type complexExpression struct {
	ExpressionInterfaceImpl
	expressions Expression
}

func complexExpr(expression Expression) Expression {
	complexExpression := &complexExpression{expressions: expression}
	complexExpression.ExpressionInterfaceImpl.Parent = complexExpression

	return complexExpression
}

func (s *complexExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if !contains(options, NoWrap) {
		out.WriteString("(")
	}

	s.expressions.serialize(statement, out, options...) // FallTrough here because complexExpression is just a wrapper

	if !contains(options, NoWrap) {
		out.WriteString(")")
	}
}

func wrap(expressions ...Expression) Expression {
	return NewFunc("", expressions, nil)
}
