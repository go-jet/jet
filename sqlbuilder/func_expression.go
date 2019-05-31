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

func (f *funcExpressionImpl) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	if f == nil {
		return errors.New("Function expression is nil. ")
	}

	out.writeString(f.name + "(")

	err := serializeExpressionList(statement, f.expression, ", ", out)
	if err != nil {
		return err
	}
	out.writeString(")")

	return nil
}

// ------------------- FLOAT FUNCTIONS --------------------------//

type floatFunc struct {
	funcExpressionImpl
	floatInterfaceImpl
}

func newFloatFunc(name string, expressions ...expression) FloatExpression {
	floatFunc := &floatFunc{}

	floatFunc.funcExpressionImpl = *newFunc(name, expressions, floatFunc)
	floatFunc.floatInterfaceImpl.parent = floatFunc

	return floatFunc
}

func COUNTf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("COUNT", floatExpression)
}

func MAXf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("MAX", floatExpression)
}

func SUMf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("SUM", floatExpression)
}

// ------------------- FLOAT FUNCTIONS --------------------------//

type integerFunc struct {
	funcExpressionImpl
	integerInterfaceImpl
}

func newIntegerFunc(name string, expressions ...expression) IntegerExpression {
	floatFunc := &integerFunc{}

	floatFunc.funcExpressionImpl = *newFunc(name, expressions, floatFunc)
	floatFunc.integerInterfaceImpl.parent = floatFunc

	return floatFunc
}

func COUNTi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("COUNT", integerExpression)
}

func MAXi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("MAX", integerExpression)
}

func SUMi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("SUM", integerExpression)
}
