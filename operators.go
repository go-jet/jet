package jet

import "errors"

//----------- Logical operators ---------------//

// Returns negation of bool expression expr
func NOT(exp BoolExpression) BoolExpression {
	return newPrefixBoolOperator(exp, "NOT")
}

func BIT_NOT(expr IntegerExpression) IntegerExpression {
	return newPrefixIntegerOperator(expr, "~")
}

//----------- Comparison operators ---------------//

func EXISTS(subQuery SelectStatement) BoolExpression {
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

type CaseOperatorExpression interface {
	Expression

	WHEN(condition Expression) CaseOperatorExpression
	THEN(then Expression) CaseOperatorExpression
	ELSE(els Expression) CaseOperatorExpression
}

type caseOperatorImpl struct {
	expressionInterfaceImpl

	expression Expression
	when       []Expression
	then       []Expression
	els        Expression
}

func CASE(expression ...Expression) CaseOperatorExpression {
	caseExp := &caseOperatorImpl{}

	if len(expression) > 0 {
		caseExp.expression = expression[0]
	}

	caseExp.expressionInterfaceImpl.parent = caseExp

	return caseExp
}

func (c *caseOperatorImpl) WHEN(when Expression) CaseOperatorExpression {
	c.when = append(c.when, when)
	return c
}

func (c *caseOperatorImpl) THEN(then Expression) CaseOperatorExpression {
	c.then = append(c.then, then)
	return c
}

func (c *caseOperatorImpl) ELSE(els Expression) CaseOperatorExpression {
	c.els = els

	return c
}

func (c *caseOperatorImpl) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	if c == nil {
		return errors.New("jet: Case Expression is nil. ")
	}

	out.writeString("(CASE")

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
		out.writeString("WHEN")
		err := when.serialize(statement, out, noWrap)

		if err != nil {
			return err
		}

		out.writeString("THEN")
		err = c.then[i].serialize(statement, out, noWrap)

		if err != nil {
			return err
		}
	}

	if c.els != nil {
		out.writeString("ELSE")
		err := c.els.serialize(statement, out, noWrap)

		if err != nil {
			return err
		}
	}

	out.writeString("END)")

	return nil
}
