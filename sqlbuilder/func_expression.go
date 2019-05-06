package sqlbuilder

import "errors"

type funcExpressionImpl struct {
	expressionInterfaceImpl

	name       string
	expression []Expression
}

func ROW(expressions ...Expression) Expression {
	return newFunc("ROW", expressions, nil)
}

func newFunc(name string, expressions []Expression, parent Expression) *funcExpressionImpl {
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

func (f *funcExpressionImpl) Serialize(out *queryData, options ...serializeOption) error {
	out.WriteString(f.name)
	out.WriteString("(")
	err := serializeExpressionList(f.expression, ", ", out)
	if err != nil {
		return err
	}
	out.WriteString(")")

	return nil
}

type numericFunc struct {
	funcExpressionImpl
	numericInterfaceImpl
}

func NewNumericFunc(name string, expressions ...Expression) NumericExpression {
	numericFunc := &numericFunc{}

	numericFunc.funcExpressionImpl = *newFunc(name, expressions, numericFunc)
	numericFunc.numericInterfaceImpl.parent = numericFunc

	return numericFunc
}

//func (f *FuncExpression) SerializeSqlForColumnList(out *bytes.Buffer) error {
//	return f.Serialize(out)
//}

func MAX(expression NumericExpression) NumericExpression {
	return NewNumericFunc("MAX", expression)
}

func SUM(expression NumericExpression) NumericExpression {
	return NewNumericFunc("SUM", expression)
}

type caseInterface interface {
	Expression

	WHEN(condition Expression) caseInterface
	THEN(then Expression) caseInterface
	ELSE(els Expression) caseInterface
}

type caseExpression struct {
	expressionInterfaceImpl

	expression Expression
	when       []Expression
	then       []Expression
	els        Expression
}

func CASE(expression ...Expression) caseInterface {
	caseExp := &caseExpression{}

	if len(expression) == 1 {
		caseExp.expression = expression[0]
	}

	caseExp.expressionInterfaceImpl.parent = caseExp

	return caseExp
}

func (c *caseExpression) WHEN(when Expression) caseInterface {
	c.when = append(c.when, when)
	return c
}

func (c *caseExpression) THEN(then Expression) caseInterface {
	c.then = append(c.then, then)
	return c
}

func (c *caseExpression) ELSE(els Expression) caseInterface {
	c.els = els

	return c
}

func (c *caseExpression) Serialize(out *queryData, options ...serializeOption) error {
	out.WriteString("(CASE")

	if c.expression != nil {
		out.WriteString(" ")
		err := c.expression.Serialize(out)

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
		out.WriteString(" WHEN ")
		err := when.Serialize(out)

		if err != nil {
			return err
		}

		out.WriteString(" THEN ")
		err = c.then[i].Serialize(out)

		if err != nil {
			return err
		}
	}

	if c.els != nil {
		out.WriteString(" ELSE ")
		err := c.els.Serialize(out)

		if err != nil {
			return err
		}
	}

	out.WriteString(" END)")

	return nil
}
