package jet

// ColumnAssigment is interface wrapper around column assignment
type ColumnAssigment interface {
	Serializer
	isColumnAssignment()
}

type columnAssigmentImpl struct {
	column   ColumnSerializer
	toAssign Serializer
}

func (a columnAssigmentImpl) isColumnAssignment() {}

func (a columnAssigmentImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	a.column.serialize(statement, out, ShortName.WithFallTrough(options)...)
	out.WriteString("=")
	a.toAssign.serialize(statement, out, FallTrough(options)...)
}
