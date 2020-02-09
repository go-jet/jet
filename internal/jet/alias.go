package jet

type alias struct {
	expression Expression
	alias      string
}

func newAlias(expression Expression, aliasName string) Projection {
	return &alias{
		expression: expression,
		alias:      aliasName,
	}
}

func (a *alias) fromImpl(subQuery SelectTable) Projection {
	column := NewColumnImpl(a.alias, "", nil)
	column.subQuery = subQuery

	return &column
}

func (a *alias) serializeForProjection(statement StatementType, out *SQLBuilder) {
	a.expression.serialize(statement, out)

	out.WriteString("AS")
	out.WriteAlias(a.alias)
}
