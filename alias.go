package jet

type alias struct {
	expression Expression
	alias      string
}

func newAlias(expression Expression, aliasName string) projection {
	return &alias{
		expression: expression,
		alias:      aliasName,
	}
}

func (a *alias) from(subQuery ExpressionTable) projection {
	column := newColumn(a.alias, "", nil)
	column.parent = &column
	column.subQuery = subQuery

	return &column
}

func (a *alias) serializeForProjection(statement statementType, out *queryData) error {
	err := a.expression.serialize(statement, out)

	if err != nil {
		return err
	}

	out.writeString("AS")
	out.writeQuotedString(a.alias)

	return nil
}
