package sqlbuilder

import "bytes"

type FuncExpression struct {
	name       string
	expression Expression

	alias string
}

func (f *FuncExpression) As(alias string) Clause {
	newFuncExpression := *f

	newFuncExpression.alias = alias

	return &newFuncExpression
}

func (f *FuncExpression) SerializeSql(out *bytes.Buffer, options ...serializeOption) error {
	out.WriteString(f.name)
	out.WriteString("(")
	err := f.expression.SerializeSql(out)
	if err != nil {
		return err
	}
	out.WriteString(")")

	if f.alias != "" {
		out.WriteString(` AS "`)
		out.WriteString(f.alias)
		out.WriteString(`"`)
	}

	return nil
}

//func (f *FuncExpression) SerializeSqlForColumnList(out *bytes.Buffer) error {
//	return f.SerializeSql(out)
//}

func MAX(expression Expression) *FuncExpression {
	return &FuncExpression{
		name:       "MAX",
		expression: expression,
	}
}

func SUM(expression Expression) *FuncExpression {
	return &FuncExpression{
		name:       "SUM",
		expression: expression,
	}
}
