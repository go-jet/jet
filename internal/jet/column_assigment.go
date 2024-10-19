package jet

// ColumnAssigment is interface wrapper around column assigment
type ColumnAssigment interface {
	Serializer
	isColumnAssigment()
}

type columnAssigmentImpl struct {
	column     ColumnSerializer
	expression Expression
}

func NewColumnAssignment(serializer ColumnSerializer, expression Expression) ColumnAssigment {
	return &columnAssigmentImpl{
		column:     serializer,
		expression: expression,
	}
}

func (a columnAssigmentImpl) isColumnAssigment() {}

func (a columnAssigmentImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	a.column.serialize(statement, out, ShortName.WithFallTrough(options)...)
	out.WriteString("=")
	a.expression.serialize(statement, out, FallTrough(options)...)
}
