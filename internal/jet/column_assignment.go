package jet

// ColumnAssignment is interface wrapper around column assignment
type ColumnAssignment interface {
	Serializer
	isColumnAssignment()
}

// Deprecated: ColumnAssigment is a typo. Use ColumnAssignment instead.
type ColumnAssigment = ColumnAssignment

type columnAssignmentImpl struct {
	column   ColumnSerializer
	toAssign Serializer
}

func (a columnAssignmentImpl) isColumnAssignment() {}

func (a columnAssignmentImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	a.column.serialize(statement, out, ShortName.WithFallTrough(options)...)
	out.WriteString("=")
	a.toAssign.serialize(statement, out, FallTrough(options)...)
}
