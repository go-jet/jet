package jet

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
	return e.Parent
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
	return newBinaryBoolOperatorExpression(e.Parent, WRAP(expressions...), "IN")
}

// NOT_IN checks if this expressions is different of all expressions in expressions list
func (e *ExpressionInterfaceImpl) NOT_IN(expressions ...Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(e.Parent, WRAP(expressions...), "NOT IN")
}

// AS the temporary alias name to assign to the expression
func (e *ExpressionInterfaceImpl) AS(alias string) Projection {
	return newAlias(e.Parent, alias)
}

// ASC expression will be used to sort query result in ascending order
func (e *ExpressionInterfaceImpl) ASC() OrderByClause {
	return newOrderByClause(e.Parent, true)
}

// DESC expression will be used to sort query result in descending order
func (e *ExpressionInterfaceImpl) DESC() OrderByClause {
	return newOrderByClause(e.Parent, false)
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
func NewBinaryOperatorExpression(lhs, rhs Serializer, operator string, additionalParam ...Expression) *binaryOperatorExpression {
	binaryExpression := &binaryOperatorExpression{
		lhs:      lhs,
		rhs:      rhs,
		operator: operator,
	}

	if len(additionalParam) > 0 {
		binaryExpression.additionalParam = additionalParam[0]
	}

	binaryExpression.ExpressionInterfaceImpl.Parent = binaryExpression

	return binaryExpression
}

func (c *binaryOperatorExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if c.lhs == nil {
		panic("jet: lhs is nil for '" + c.operator + "' operator")
	}
	if c.rhs == nil {
		panic("jet: rhs is nil for '" + c.operator + "' operator")
	}

	wrap := !contains(options, NoWrap)

	if wrap {
		out.WriteString("(")
	}

	if serializeOverride := out.Dialect.OperatorSerializeOverride(c.operator); serializeOverride != nil {
		serializeOverrideFunc := serializeOverride(c.lhs, c.rhs, c.additionalParam)
		serializeOverrideFunc(statement, out, FallTrough(options)...)
	} else {
		c.lhs.serialize(statement, out, FallTrough(options)...)
		out.WriteString(c.operator)
		c.rhs.serialize(statement, out, FallTrough(options)...)
	}

	if wrap {
		out.WriteString(")")
	}
}

// A prefix operator Expression
type prefixExpression struct {
	ExpressionInterfaceImpl

	expression Expression
	operator   string
}

func newPrefixOperatorExpression(expression Expression, operator string) *prefixExpression {
	prefixExpression := &prefixExpression{
		expression: expression,
		operator:   operator,
	}
	prefixExpression.ExpressionInterfaceImpl.Parent = prefixExpression

	return prefixExpression
}

func (p *prefixExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("(")
	out.WriteString(p.operator)

	if p.expression == nil {
		panic("jet: nil prefix expression in prefix operator " + p.operator)
	}

	p.expression.serialize(statement, out, FallTrough(options)...)

	out.WriteString(")")
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
	if p.expression == nil {
		panic("jet: nil prefix expression in postfix operator " + p.operator)
	}

	p.expression.serialize(statement, out, FallTrough(options)...)

	out.WriteString(p.operator)
}
