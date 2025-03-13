package jet

type expressionOrColumnList interface {
	Serializer
	isExpressionOrColumnList()
}

type columnListAssigment []ColumnAssigment

func (c columnListAssigment) isColumnAssignment() {}

func (c columnListAssigment) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	for i, columnAssigment := range c {
		if i > 0 {
			out.WriteString(",")
			out.NewLine()
		}

		columnAssigment.serialize(statement, out, options...)
	}
}
