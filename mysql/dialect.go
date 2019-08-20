package mysql

import (
	"github.com/go-jet/jet/internal/jet"
)

// Dialect is implementation of MySQL dialect for SQL Builder serialisation.
var Dialect = newDialect()

func newDialect() jet.Dialect {

	operatorSerializeOverrides := map[string]jet.SerializeOverride{}
	operatorSerializeOverrides[jet.StringRegexpLikeOperator] = mysql_REGEXP_LIKE_operator
	operatorSerializeOverrides[jet.StringNotRegexpLikeOperator] = mysql_NOT_REGEXP_LIKE_operator
	operatorSerializeOverrides["IS DISTINCT FROM"] = mysql_IS_DISTINCT_FROM
	operatorSerializeOverrides["IS NOT DISTINCT FROM"] = mysql_IS_NOT_DISTINCT_FROM
	operatorSerializeOverrides["/"] = mysql_DIVISION
	operatorSerializeOverrides["#"] = mysql_BIT_XOR
	operatorSerializeOverrides[jet.StringConcatOperator] = mysql_CONCAT_operator

	mySQLDialectParams := jet.DialectParams{
		Name:                       "MySQL",
		PackageName:                "mysql",
		OperatorSerializeOverrides: operatorSerializeOverrides,
		AliasQuoteChar:             '"',
		IdentifierQuoteChar:        '`',
		ArgumentPlaceholder: func(int) string {
			return "?"
		},
	}

	return jet.NewDialect(mySQLDialectParams)
}

func mysql_BIT_XOR(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
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
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
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
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
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
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
			panic("jet: invalid number of expressions for operator")
		}

		jet.Serialize(expressions[0], statement, out)
		out.WriteString("<=>")
		jet.Serialize(expressions[1], statement, out)
	}
}

func mysql_IS_DISTINCT_FROM(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		out.WriteString("NOT(")
		mysql_IS_NOT_DISTINCT_FROM(expressions...)(statement, out, options...)
		out.WriteString(")")
	}
}

func mysql_REGEXP_LIKE_operator(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
			panic("jet: invalid number of expressions for operator")
		}

		jet.Serialize(expressions[0], statement, out, options...)

		caseSensitive := false

		if len(expressions) >= 3 {
			if stringLiteral, ok := expressions[2].(jet.LiteralExpression); ok {
				caseSensitive = stringLiteral.Value().(bool)
			}
		}

		out.WriteString("REGEXP")

		if caseSensitive {
			out.WriteString("BINARY")
		}

		jet.Serialize(expressions[1], statement, out, options...)
	}
}

func mysql_NOT_REGEXP_LIKE_operator(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
			panic("jet: invalid number of expressions for operator")
		}

		jet.Serialize(expressions[0], statement, out, options...)

		caseSensitive := false

		if len(expressions) >= 3 {
			if stringLiteral, ok := expressions[2].(jet.LiteralExpression); ok {
				caseSensitive = stringLiteral.Value().(bool)
			}
		}

		out.WriteString("NOT REGEXP")

		if caseSensitive {
			out.WriteString("BINARY")
		}

		jet.Serialize(expressions[1], statement, out, options...)
	}
}
