package jet

// Values hold a set of one or more rows
type Values []RowExpression

func (v Values) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteByte('(')
	out.IncreaseIdent(5)

	out.NewLine()
	out.WriteString("VALUES")

	for rowIndex, row := range v {
		if rowIndex > 0 {
			out.WriteString(",")
			out.NewLine()
		} else {
			out.IncreaseIdent(7)
		}

		row.serialize(statement, out, options...)
	}
	out.DecreaseIdent(7)
	out.DecreaseIdent(5)
	out.NewLine()
	out.WriteByte(')')
}

func (v Values) projections() ProjectionList {
	if len(v) == 0 {
		return nil
	}

	return v[0].projections()
}
