package sqlbuilder

import "bytes"

type Alias struct {
	Clause

	expression Expression
	alias      string
}

func NewAlias(expression Expression, alias string) *Alias {
	if !validIdentifierName(alias) {
		panic("Invalid alias")
	}

	return &Alias{
		expression: expression,
		alias:      alias,
	}
}

func (a *Alias) SerializeSql(out *bytes.Buffer, options ...serializeOption) error {

	err := a.expression.SerializeSql(out, ALIASED)

	if err != nil {
		return err
	}

	out.WriteString(" AS \"" + a.alias + "\"")

	return nil
}
