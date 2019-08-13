package mysql

import (
	"github.com/go-jet/jet/internal/jet"
)

var Dialect = NewDialect()

func NewDialect() jet.Dialect {

	serializeOverrides := map[string]jet.SerializeOverride{}
	serializeOverrides["IS DISTINCT FROM"] = mysql_IS_DISTINCT_FROM
	serializeOverrides["IS NOT DISTINCT FROM"] = mysql_IS_NOT_DISTINCT_FROM
	serializeOverrides["/"] = mysql_DIVISION
	serializeOverrides["#"] = mysql_BIT_XOR
	serializeOverrides[jet.StringConcatOperator] = mysql_CONCAT_operator

	mySQLDialectParams := jet.DialectParams{
		Name:                "MySQL",
		PackageName:         "mysql",
		SerializeOverrides:  serializeOverrides,
		AliasQuoteChar:      '"',
		IdentifierQuoteChar: '`',
		ArgumentPlaceholder: func(int) string {
			return "?"
		},
	}

	return jet.NewDialect(mySQLDialectParams)
}

func mysql_BIT_XOR(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) {
		if len(expressions) != 2 {
			panic("jet: invalid number of expressions for operator XOR")
		}

		lhs := expressions[0]
		rhs := expressions[1]

		jet.Serialize(lhs, statement, out, options...)

		out.WriteString("^")

		jet.Serialize(rhs, statement, out, options...)
	}
}

func mysql_CONCAT_operator(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) {
		if len(expressions) != 2 {
			panic("jet: invalid number of expressions for operator CONCAT")
		}
		out.WriteString("CONCAT(")

		jet.Serialize(expressions[0], statement, out, options...)

		out.WriteString(", ")

		jet.Serialize(expressions[1], statement, out, options...)

		out.WriteString(")")
	}
}

func mysql_DIVISION(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) {
		if len(expressions) != 2 {
			panic("jet: invalid number of expressions for operator DIV")
		}

		lhs := expressions[0]
		rhs := expressions[1]

		jet.Serialize(lhs, statement, out, options...)

		_, isLhsInt := lhs.(IntegerExpression)
		_, isRhsInt := rhs.(IntegerExpression)

		if isLhsInt && isRhsInt {
			out.WriteString("DIV")
		} else {
			out.WriteString("/")
		}

		jet.Serialize(rhs, statement, out, options...)
	}
}

func mysql_IS_NOT_DISTINCT_FROM(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) {
		if len(expressions) != 2 {
			panic("jet: invalid number of expressions for operator")
		}

		jet.Serialize(expressions[0], statement, out)
		out.WriteString("<=>")
		jet.Serialize(expressions[1], statement, out)
	}
}

func mysql_IS_DISTINCT_FROM(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) {
		out.WriteString("NOT(")
		mysql_IS_NOT_DISTINCT_FROM(expressions...)(statement, out, options...)
		out.WriteString(")")
	}
}
