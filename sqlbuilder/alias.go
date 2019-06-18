package sqlbuilder

type alias struct {
	expression Expression
	alias      string

	subQuery ExpressionTable
}

func newAlias(expression Expression, aliasName string) projection {
	return &alias{
		expression: expression,
		alias:      aliasName,
	}
}

func (a *alias) from(subQuery ExpressionTable) projection {
	newAlias := *a
	newAlias.subQuery = subQuery
	return &newAlias
}

func (a *alias) serializeForProjection(statement statementType, out *queryData) error {
	if a.subQuery != nil {
		out.writeIdentifier(a.subQuery.Alias())
		out.writeByte('.')
		out.writeQuotedString(a.alias)
	} else {
		err := a.expression.serialize(statement, out)

		if err != nil {
			return err
		}
	}

	out.writeString(`AS "` + a.alias + `"`)

	return nil
}
