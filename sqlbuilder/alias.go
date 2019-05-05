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

func (a *Alias) SerializeForProjection(out *queryData) error {

	err := a.expression.Serialize(out)

	if err != nil {
		return err
	}

	out.WriteString(" AS \"" + a.alias + "\"")

	return nil
}
