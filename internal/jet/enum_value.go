package jet

type enumValue struct {
	ExpressionInterfaceImpl
	stringInterfaceImpl

	name string
}

// NewEnumValue creates new named enum value
func NewEnumValue(name string) StringExpression {
	enumValue := &enumValue{name: name}

	enumValue.ExpressionInterfaceImpl.Parent = enumValue
	enumValue.stringInterfaceImpl.parent = enumValue

	return enumValue
}

func (e enumValue) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.insertConstantArgument(e.name)
}
