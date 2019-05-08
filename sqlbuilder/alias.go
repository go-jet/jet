package sqlbuilder

type Alias struct {
	expression expression
	alias      string
}

func NewAlias(expression expression, alias string) *Alias {
	return &Alias{
		expression: expression,
		alias:      alias,
	}
}

func (a *Alias) serializeForProjection(statement statementType, out *queryData) error {

	err := a.expression.serializeForProjection(statement, out)

	if err != nil {
		return err
	}

	out.writeString(" AS \"" + a.alias + "\"")

	return nil
}
