package jet

type expressionOrColumnList interface {
	Serializer
	isExpressionOrColumnList()
}

type columnListAssignment []ColumnAssignment

func (c columnListAssignment) isColumnAssignment() {}

func (c columnListAssignment) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	for i, columnAssignment := range c {
		if i > 0 {
			out.WriteString(",")
			out.NewLine()
		}

		columnAssignment.serialize(statement, out, options...)
	}
}
