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
	column := newColumn(a.alias, "", nil)
	column.parent = &column
	column.subQuery = subQuery

	return &column
}

func (a *alias) serializeForProjection(statement StatementType, out *SqlBuilder) error {
	err := a.expression.serialize(statement, out)

	if err != nil {
		return err
	}

	out.WriteString("AS")
	out.writeAlias(a.alias)

	return nil
}
