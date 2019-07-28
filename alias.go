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

func (a *alias) from(subQuery SelectTable) projection {
	column := newColumn(a.alias, "", nil)
	column.parent = &column
	column.subQuery = subQuery

	return &column
}

func (a *alias) serializeForProjection(statement statementType, out *sqlBuilder) error {
	err := a.expression.serialize(statement, out)

	if err != nil {
		return err
	}

	out.writeString("AS")
	out.writeAlias(a.alias)

	return nil
}
