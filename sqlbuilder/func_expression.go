package sqlbuilder

import "errors"

type funcExpressionImpl struct {
	expressionInterfaceImpl

	name       string
	expression []expression
}

func ROW(expressions ...expression) expression {
	return newFunc("ROW", expressions, nil)
}

func newFunc(name string, expressions []expression, parent expression) *funcExpressionImpl {
	funcExp := &funcExpressionImpl{
		name:       name,
		expression: expressions,
	}

	if parent != nil {
		funcExp.expressionInterfaceImpl.parent = parent
	} else {
		funcExp.expressionInterfaceImpl.parent = funcExp
	}

	return funcExp
}

func (f *funcExpressionImpl) serialize(statement statementType, out *queryData) error {
	out.writeString(f.name)
	out.writeString("(")
	err := serializeExpressionList(statement, f.expression, ", ", out)
	if err != nil {
		return err
	}
	out.writeString(")")

	return nil
}

type numericFunc struct {
	funcExpressionImpl
	numericInterfaceImpl
}

func NewNumericFunc(name string, expressions ...expression) numericExpression {
	numericFunc := &numericFunc{}

	numericFunc.funcExpressionImpl = *newFunc(name, expressions, numericFunc)
	numericFunc.numericInterfaceImpl.parent = numericFunc

	return numericFunc
}

func MAX(expression numericExpression) numericExpression {
	return NewNumericFunc("MAX", expression)
}

func SUM(expression numericExpression) numericExpression {
	return NewNumericFunc("SUM", expression)
}

type caseInterface interface {
	expression

	WHEN(condition expression) caseInterface
	THEN(then expression) caseInterface
	ELSE(els expression) caseInterface
}

type caseExpression struct {
	expressionInterfaceImpl

	expression expression
	when       []expression
	then       []expression
	els        expression
}

func CASE(expression ...expression) caseInterface {
	caseExp := &caseExpression{}

	if len(expression) == 1 {
		caseExp.expression = expression[0]
	}

	caseExp.expressionInterfaceImpl.parent = caseExp

	return caseExp
}

func (c *caseExpression) WHEN(when expression) caseInterface {
	c.when = append(c.when, when)
	return c
}

func (c *caseExpression) THEN(then expression) caseInterface {
	c.then = append(c.then, then)
	return c
}

func (c *caseExpression) ELSE(els expression) caseInterface {
	c.els = els

	return c
}

func (c *caseExpression) serialize(statement statementType, out *queryData) error {
	out.writeString("(CASE")

	if c.expression != nil {
		out.writeString(" ")
		err := c.expression.serialize(statement, out)

		if err != nil {
			return err
		}
	}

	if len(c.when) == 0 || len(c.then) == 0 {
		return errors.New("Invalid case statement. There should be at least one when/then expression pair. ")
	}

	if len(c.when) != len(c.then) {
		return errors.New("When and then expression count mismatch. ")
	}

	for i, when := range c.when {
		out.writeString(" WHEN ")
		err := when.serialize(statement, out)

		if err != nil {
			return err
		}

		out.writeString(" THEN ")
		err = c.then[i].serialize(statement, out)

		if err != nil {
			return err
		}
	}

	if c.els != nil {
		out.writeString(" ELSE ")
		err := c.els.serialize(statement, out)

		if err != nil {
			return err
		}
	}

	out.writeString(" END)")

	return nil
}
