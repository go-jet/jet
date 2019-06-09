package sqlbuilder

type Alias struct {
	expression Expression
	alias      string
}

func NewAlias(expression Expression, alias string) *Alias {
	return &Alias{
		expression: expression,
		alias:      alias,
	}
}

func (a *Alias) serializeForProjection(statement statementType, out *queryData) error {

	err := a.expression.serialize(statement, out)

	if err != nil {
		return err
	}

	out.writeString(`AS "` + a.alias + `"`)

	return nil
}
