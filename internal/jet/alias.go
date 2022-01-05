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
	// if alias is in the form "table.column", we break it into two parts so that ProjectionList.As(newAlias) can
	// overwrite tableName with a new alias. This method is called only for exporting aliased custom columns.
	// Generated columns have default aliasing.
	tableName, columnName := extractTableAndColumnName(a.alias)

	column := NewColumnImpl(columnName, tableName, nil)
	column.subQuery = subQuery

	return &column
}

func (a *alias) serializeForProjection(statement StatementType, out *SQLBuilder) {
	a.expression.serialize(statement, out)

	out.WriteString("AS")
	out.WriteAlias(a.alias)
}
