package sqlbuilder

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
