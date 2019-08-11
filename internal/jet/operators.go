package jet

import "errors"

//----------- Logical operators ---------------//

// NOT returns negation of bool expression result
func NOT(exp BoolExpression) BoolExpression {
	return newPrefixBoolOperator(exp, "NOT")
}

// BIT_NOT inverts every bit in integer expression result
func BIT_NOT(expr IntegerExpression) IntegerExpression {
	return newPrefixIntegerOperator(expr, "~")
}

//----------- Comparison operators ---------------//

// EXISTS checks for existence of the rows in subQuery
func EXISTS(subQuery Expression) BoolExpression {
	return newPrefixBoolOperator(subQuery, "EXISTS")
}

// Returns a representation of "a=b"
func eq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperator(lhs, rhs, "=")
}

// Returns a representation of "a!=b"
func notEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperator(lhs, rhs, "!=")
}

func isDistinctFrom(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperator(lhs, rhs, "IS DISTINCT FROM")
}

func isNotDistinctFrom(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperator(lhs, rhs, "IS NOT DISTINCT FROM")
}

// Returns a representation of "a<b"
func lt(lhs Expression, rhs Expression) BoolExpression {
	return newBinaryBoolOperator(lhs, rhs, "<")
}

// Returns a representation of "a<=b"
func ltEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperator(lhs, rhs, "<=")
}

// Returns a representation of "a>b"
func gt(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperator(lhs, rhs, ">")
}

// Returns a representation of "a>=b"
func gtEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolOperator(lhs, rhs, ">=")
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

func (c *caseOperatorImpl) accept(visitor visitor) {
	visitor.visit(c)

	c.expression.accept(visitor)

	for _, when := range c.when {
		when.accept(visitor)
	}

	for _, then := range c.then {
		then.accept(visitor)
	}

	if c.els != nil {
		c.els.accept(visitor)
	}
}

func (c *caseOperatorImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	if c == nil {
		return errors.New("jet: Case Expression is nil. ")
	}

	out.WriteString("(CASE")

	if c.expression != nil {
		err := c.expression.serialize(statement, out)

		if err != nil {
			return err
		}
	}

	if len(c.when) == 0 || len(c.then) == 0 {
		return errors.New("jet: Invalid case Statement. There should be at least one when/then Expression pair. ")
	}

	if len(c.when) != len(c.then) {
		return errors.New("jet: When and then Expression count mismatch. ")
	}

	for i, when := range c.when {
		out.WriteString("WHEN")
		err := when.serialize(statement, out, noWrap)

		if err != nil {
			return err
		}

		out.WriteString("THEN")
		err = c.then[i].serialize(statement, out, noWrap)

		if err != nil {
			return err
		}
	}

	if c.els != nil {
		out.WriteString("ELSE")
		err := c.els.serialize(statement, out, noWrap)

		if err != nil {
			return err
		}
	}

	out.WriteString("END)")

	return nil
}
