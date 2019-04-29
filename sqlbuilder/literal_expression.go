package sqlbuilder

// Representation of an escaped literal
type literalExpression struct {
	expressionInterfaceImpl
	value interface{}
}

func Literal(value interface{}) *literalExpression {
	exp := literalExpression{value: value}
	exp.expressionInterfaceImpl.parent = &exp

	return &exp
}

func (l literalExpression) Serialize(out *queryData, options ...serializeOption) error {
	//sqltypes.Value(c.value).EncodeSql(out)

	out.InsertArgument(l.value)

	return nil
}
