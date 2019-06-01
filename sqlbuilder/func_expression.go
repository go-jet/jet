package sqlbuilder

import "errors"

type funcExpressionImpl struct {
	expressionInterfaceImpl

	name        string
	expressions []expression
}

func ROW(expressions ...expression) expression {
	return newFunc("ROW", expressions, nil)
}

func newFunc(name string, expressions []expression, parent expression) *funcExpressionImpl {
	funcExp := &funcExpressionImpl{
		name:        name,
		expressions: expressions,
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
		return errors.New("Function expressions is nil. ")
	}

	out.writeString(f.name + "(")

	err := serializeExpressionList(statement, f.expressions, ", ", out)
	if err != nil {
		return err
	}
	out.writeString(")")

	return nil
}

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

// ------------------ Mathematical functions ---------------//

func ABSf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("ABS", floatExpression)
}

func ABSi(integerExpression IntegerExpression) FloatExpression {
	return newFloatFunc("ABS", integerExpression)
}

func SQRTf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("SQRT", floatExpression)
}

func SQRTi(integerExpression IntegerExpression) FloatExpression {
	return newFloatFunc("SQRT", integerExpression)
}

func CBRTf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("CBRT", floatExpression)
}

func CBRTi(integerExpression IntegerExpression) FloatExpression {
	return newFloatFunc("CBRT", integerExpression)
}

func CEIL(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("CEIL", floatExpression)
}

func FLOOR(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("FLOOR", floatExpression)
}

func ROUND(floatExpression FloatExpression, intExpression ...IntegerExpression) FloatExpression {
	if len(intExpression) > 0 {
		return newFloatFunc("ROUND", floatExpression, intExpression[0])
	}
	return newFloatFunc("ROUND", floatExpression)
}

func SIGN(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("SIGN", floatExpression)
}

func TRUNC(floatExpression FloatExpression, intExpression ...IntegerExpression) FloatExpression {
	if len(intExpression) > 0 {
		return newFloatFunc("TRUNC", floatExpression, intExpression[0])
	}
	return newFloatFunc("TRUNC", floatExpression)
}

func LN(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("LN", floatExpression)
}

func LOG(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("LOG", floatExpression)
}

// ----------------- Group function operators -------------------//

func MAXf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("MAX", floatExpression)
}

func MAXi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("MAX", integerExpression)
}

func SUMf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("SUM", floatExpression)
}

func SUMi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("SUM", integerExpression)
}

func COUNTf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("COUNT", floatExpression)
}

func COUNTi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("COUNT", integerExpression)
}
