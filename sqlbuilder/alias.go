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

func (a *Alias) serializeForProjection(out *queryData) error {

	err := a.expression.serializeForProjection(out)

	if err != nil {
		return err
	}

	out.WriteString(" AS \"" + a.alias + "\"")

	return nil
}
