package sqlbuilder

import "errors"

//----------- Logical operators ---------------//

// Returns a representation of "not expr"
func NOT(expr BoolExpression) BoolExpression {
	return newPrefixBoolExpression(expr, "NOT")
}

//----------- Comparison operators ---------------//

// Returns a representation of "a=b"
func EQ(lhs, rhs expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "=")
}

// Returns a representation of "a!=b"
func NOT_EQ(lhs, rhs expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "!=")
}

func IS_DISTINCT_FROM(lhs, rhs expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "IS DISTINCT FROM")
}

func IS_NOT_DISTINCT_FROM(lhs, rhs expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "IS NOT DISTINCT FROM")
}

// Returns a representation of "a<b"
func LT(lhs expression, rhs expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "<")
}

// Returns a representation of "a<=b"
func LT_EQ(lhs, rhs expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "<=")
}

// Returns a representation of "a>b"
func GT(lhs, rhs expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, ">")
}

// Returns a representation of "a>=b"
func GT_EQ(lhs, rhs expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, ">=")
}

func IS_TRUE(expr BoolExpression) BoolExpression {
	return newPostifxBoolExpression(expr, "IS TRUE")
}

func IS_NOT_TRUE(expr BoolExpression) BoolExpression {
	return newPostifxBoolExpression(expr, "IS NOT TRUE")
}

func IS_FALSE(expr BoolExpression) BoolExpression {
	return newPostifxBoolExpression(expr, "IS FALSE")
}

func IS_NOT_FALSE(expr BoolExpression) BoolExpression {
	return newPostifxBoolExpression(expr, "IS NOT FALSE")
}

func IS_UNKNOWN(expr BoolExpression) BoolExpression {
	return newPostifxBoolExpression(expr, "IS UNKNOWN")
}

func IS_NOT_UNKNOWN(expr BoolExpression) BoolExpression {
	return newPostifxBoolExpression(expr, "IS NOT UNKNOWN")
}

func And(lhs, rhs expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "AND")
}

// Returns a representation of "c[0] OR ... OR c[n-1]" for c in clauses
func Or(lhs, rhs expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "OR")
}

func Regexp(lhs, rhs expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "REGEXP")
}

func RegexpL(lhs expression, val string) BoolExpression {
	return Regexp(lhs, literal(val))
}

func EXISTS(subQuery selectStatement) BoolExpression {
	return newPrefixBoolExpression(subQuery, "EXISTS")
}

// --------------- CASE operator -------------------//

type caseOperatorExpression interface {
	expression

	WHEN(condition expression) caseOperatorExpression
	THEN(then expression) caseOperatorExpression
	ELSE(els expression) caseOperatorExpression
}

type caseOperatorImpl struct {
	expressionInterfaceImpl

	expression expression
	when       []expression
	then       []expression
	els        expression
}

func CASE(expression ...expression) caseOperatorExpression {
	caseExp := &caseOperatorImpl{}

	if len(expression) > 0 {
		caseExp.expression = expression[0]
	}

	caseExp.expressionInterfaceImpl.parent = caseExp

	return caseExp
}

func (c *caseOperatorImpl) WHEN(when expression) caseOperatorExpression {
	c.when = append(c.when, when)
	return c
}

func (c *caseOperatorImpl) THEN(then expression) caseOperatorExpression {
	c.then = append(c.then, then)
	return c
}

func (c *caseOperatorImpl) ELSE(els expression) caseOperatorExpression {
	c.els = els

	return c
}

func (c *caseOperatorImpl) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	if c == nil {
		return errors.New("Case expression is nil. ")
	}

	out.writeString("(CASE")

	if c.expression != nil {
		err := c.expression.serialize(statement, out)

		if err != nil {
			return err
		}
	}

	if len(c.when) == 0 || len(c.then) == 0 {
		return errors.New("Invalid case Statement. There should be at least one when/then expression pair. ")
	}

	if len(c.when) != len(c.then) {
		return errors.New("When and then expression count mismatch. ")
	}

	for i, when := range c.when {
		out.writeString("WHEN")
		err := when.serialize(statement, out, NO_WRAP)

		if err != nil {
			return err
		}

		out.writeString("THEN")
		err = c.then[i].serialize(statement, out, NO_WRAP)

		if err != nil {
			return err
		}
	}

	if c.els != nil {
		out.writeString("ELSE")
		err := c.els.serialize(statement, out, NO_WRAP)

		if err != nil {
			return err
		}
	}

	out.writeString("END)")

	return nil
}
