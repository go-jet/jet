package jet

// Operators
const (
	StringConcatOperator        = "||"
	StringRegexpLikeOperator    = "REGEXP"
	StringNotRegexpLikeOperator = "NOT REGEXP"
)

//----------- Logical operators ---------------//

// NOT returns negation of bool expression result
func NOT(exp BoolExpression) BoolExpression {
	return newPrefixBoolOperatorExpression(exp, "NOT")
}

// BIT_NOT inverts every bit in integer expression result
func BIT_NOT(expr IntegerExpression) IntegerExpression {
	if literalExp, ok := expr.(LiteralExpression); ok {
		literalExp.SetConstant(true)
	}
	return newPrefixIntegerOperatorExpression(expr, "~")
}

//----------- Comparison operators ---------------//

// EXISTS checks for existence of the rows in subQuery
func EXISTS(subQuery Expression) BoolExpression {
	return newPrefixBoolOperatorExpression(subQuery, "EXISTS")
}

// Returns a representation of "a=b"
func eq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(lhs, rhs, "=")
}

// Returns a representation of "a!=b"
func notEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(lhs, rhs, "!=")
}

func isDistinctFrom(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(lhs, rhs, "IS DISTINCT FROM")
}

func isNotDistinctFrom(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(lhs, rhs, "IS NOT DISTINCT FROM")
}

// Returns a representation of "a<b"
func lt(lhs Expression, rhs Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(lhs, rhs, "<")
}

// Returns a representation of "a<=b"
func ltEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(lhs, rhs, "<=")
}

// Returns a representation of "a>b"
func gt(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(lhs, rhs, ">")
}

// Returns a representation of "a>=b"
func gtEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperatorExpression(lhs, rhs, ">=")
}

// --------------- CASE operator -------------------//

// CaseOperator is interface for SQL case operator
type CaseOperator interface {
	Expression

	WHEN(condition Expression) CaseOperator
	THEN(then Expression) CaseOperator
	ELSE(els Expression) CaseOperator
}

type caseOperatorImpl struct {
	ExpressionInterfaceImpl

	expression Expression
	when       []Expression
	then       []Expression
	els        Expression
}

// CASE create CASE operator with optional list of expressions
func CASE(expression ...Expression) CaseOperator {
	caseExp := &caseOperatorImpl{}

	if len(expression) > 0 {
		caseExp.expression = expression[0]
	}

	caseExp.ExpressionInterfaceImpl.Parent = caseExp

	return caseExp
}

func (c *caseOperatorImpl) WHEN(when Expression) CaseOperator {
	c.when = append(c.when, when)
	return c
}

func (c *caseOperatorImpl) THEN(then Expression) CaseOperator {
	c.then = append(c.then, then)
	return c
}

func (c *caseOperatorImpl) ELSE(els Expression) CaseOperator {
	c.els = els

	return c
}

func (c *caseOperatorImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("(CASE")

	if c.expression != nil {
		c.expression.serialize(statement, out)
	}

	if len(c.when) == 0 || len(c.then) == 0 {
		panic("jet: invalid case Statement. There should be at least one WHEN/THEN pair. ")
	}

	if len(c.when) != len(c.then) {
		panic("jet: WHEN and THEN expression count mismatch. ")
	}

	for i, when := range c.when {
		out.WriteString("WHEN")
		when.serialize(statement, out, noWrap)

		out.WriteString("THEN")
		c.then[i].serialize(statement, out, noWrap)
	}

	if c.els != nil {
		out.WriteString("ELSE")
		c.els.serialize(statement, out, noWrap)
	}

	out.WriteString("END)")
}
