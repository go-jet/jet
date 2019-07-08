package jet

type enumValue struct {
	expressionInterfaceImpl
	stringInterfaceImpl
	name string
}

func NewEnumValue(name string) StringExpression {
	enumValue := &enumValue{name: name}

	enumValue.expressionInterfaceImpl.parent = enumValue
	enumValue.stringInterfaceImpl.parent = enumValue

	return enumValue
}

func (e enumValue) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	out.insertConstantArgument(e.name)
	return nil
}
