package jet

// Expression is common interface for all expressions.
// Can be Bool, Int, Float, String, Date, Time, Timez, Timestamp or Timestampz expressions.
type Expression interface {
	Serializer
	Projection
	GroupByClause
	OrderByClause

	// Test expression whether it is a NULL value.
	IS_NULL() BoolExpression
	// Test expression whether it is a non-NULL value.
	IS_NOT_NULL() BoolExpression

	// Check if this expressions matches any in expressions list
	IN(expressions ...Expression) BoolExpression
	// Check if this expressions is different of all expressions in expressions list
	NOT_IN(expressions ...Expression) BoolExpression

	// The temporary alias name to assign to the expression
	AS(alias string) Projection

	// Expression will be used to sort query result in ascending order
	ASC() OrderByClause
	// Expression will be used to sort query result in ascending order
	DESC() OrderByClause
}

type expressionInterfaceImpl struct {
	Parent Expression
}

func (e *expressionInterfaceImpl) fromImpl(subQuery SelectTable) Projection {
	return e.Parent
}

func (e *expressionInterfaceImpl) IS_NULL() BoolExpression {
	return newPostifxBoolExpression(e.Parent, "IS NULL")
}

func (e *expressionInterfaceImpl) IS_NOT_NULL() BoolExpression {
	return newPostifxBoolExpression(e.Parent, "IS NOT NULL")
}

func (e *expressionInterfaceImpl) IN(expressions ...Expression) BoolExpression {
	return newBinaryBoolOperator(e.Parent, WRAP(expressions...), "IN")
}

func (e *expressionInterfaceImpl) NOT_IN(expressions ...Expression) BoolExpression {
	return newBinaryBoolOperator(e.Parent, WRAP(expressions...), "NOT IN")
}

func (e *expressionInterfaceImpl) AS(alias string) Projection {
	return newAlias(e.Parent, alias)
}

func (e *expressionInterfaceImpl) ASC() OrderByClause {
	return newOrderByClause(e.Parent, true)
}

func (e *expressionInterfaceImpl) DESC() OrderByClause {
	return newOrderByClause(e.Parent, false)
}

func (e *expressionInterfaceImpl) serializeForGroupBy(statement StatementType, out *SQLBuilder) {
	e.Parent.serialize(statement, out, noWrap)
}

func (e *expressionInterfaceImpl) serializeForProjection(statement StatementType, out *SQLBuilder) {
	e.Parent.serialize(statement, out, noWrap)
}

func (e *expressionInterfaceImpl) serializeForOrderBy(statement StatementType, out *SQLBuilder) {
	e.Parent.serialize(statement, out, noWrap)
}

// Representation of binary operations (e.g. comparisons, arithmetic)
type binaryOpExpression struct {
	lhs, rhs        Expression
	additionalParam Expression
	operator        string
}

func newBinaryExpression(lhs, rhs Expression, operator string, additionalParam ...Expression) binaryOpExpression {
	binaryExpression := binaryOpExpression{
		lhs:      lhs,
		rhs:      rhs,
		operator: operator,
	}

	if len(additionalParam) > 0 {
		binaryExpression.additionalParam = additionalParam[0]
	}

	return binaryExpression
}

func (c *binaryOpExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if c.lhs == nil {
		panic("jet: lhs is nil for '" + c.operator + "' operator")
	}
	if c.rhs == nil {
		panic("jet: rhs is nil for '" + c.operator + "' operator")
	}

	wrap := !contains(options, noWrap)

	if wrap {
		out.WriteString("(")
	}

	if serializeOverride := out.Dialect.OperatorSerializeOverride(c.operator); serializeOverride != nil {
		serializeOverrideFunc := serializeOverride(c.lhs, c.rhs, c.additionalParam)
		serializeOverrideFunc(statement, out, options...)
	} else {
		c.lhs.serialize(statement, out)
		out.WriteString(c.operator)
		c.rhs.serialize(statement, out)
	}

	if wrap {
		out.WriteString(")")
	}
}

// A prefix operator Expression
type prefixOpExpression struct {
	expression Expression
	operator   string
}

func newPrefixExpression(expression Expression, operator string) prefixOpExpression {
	prefixExpression := prefixOpExpression{
		expression: expression,
		operator:   operator,
	}

	return prefixExpression
}

func (p *prefixOpExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("(")
	out.WriteString(p.operator)

	if p.expression == nil {
		panic("jet: nil prefix expression in prefix operator " + p.operator)
	}

	p.expression.serialize(statement, out)

	out.WriteString(")")
}

// A postifx operator Expression
type postfixOpExpression struct {
	expression Expression
	operator   string
}

func newPostfixOpExpression(expression Expression, operator string) postfixOpExpression {
	postfixOpExpression := postfixOpExpression{
		expression: expression,
		operator:   operator,
	}

	return postfixOpExpression
}

func (p *postfixOpExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if p.expression == nil {
		panic("jet: nil prefix expression in postfix operator " + p.operator)
	}

	p.expression.serialize(statement, out)

	out.WriteString(p.operator)
}
