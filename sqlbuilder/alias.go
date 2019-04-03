package sqlbuilder

import "bytes"

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

func (a *Alias) SerializeForProjection(out *bytes.Buffer) error {

	err := a.expression.SerializeSql(out, ALIASED)

	if err != nil {
		return err
	}

	out.WriteString(" AS \"" + a.alias + "\"")

	return nil
}
