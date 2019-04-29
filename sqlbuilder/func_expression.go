package sqlbuilder

type FuncExpression interface {
	Expression
}

type numericFunc struct {
	expressionInterfaceImpl
	numericInterfaceImpl

	name       string
	expression Expression
}

func NewNumericFunc(name string, expression Expression) NumericExpression {
	numericFunc := &numericFunc{
		name:       name,
		expression: expression,
	}

	numericFunc.expressionInterfaceImpl.parent = numericFunc
	numericFunc.numericInterfaceImpl.parent = numericFunc

	return numericFunc
}

func (f *numericFunc) Serialize(out *queryData, options ...serializeOption) error {
	out.WriteString(f.name)
	out.WriteString("(")
	err := f.expression.Serialize(out)
	if err != nil {
		return err
	}
	out.WriteString(")")

	return nil
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
