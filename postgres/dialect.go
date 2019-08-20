package postgres

import (
	"github.com/go-jet/jet/internal/jet"
	"strconv"
)

// Dialect is implementation of postgres dialect for SQL Builder serialisation.
var Dialect = newDialect()

func newDialect() jet.Dialect {

	operatorSerializeOverrides := map[string]jet.SerializeOverride{}
	operatorSerializeOverrides[jet.StringRegexpLikeOperator] = postgres_REGEXP_LIKE_operator
	operatorSerializeOverrides[jet.StringNotRegexpLikeOperator] = postgres_NOT_REGEXP_LIKE_operator
	operatorSerializeOverrides["CAST"] = postgresCAST

	dialectParams := jet.DialectParams{
		Name:                       "PostgreSQL",
		PackageName:                "postgres",
		OperatorSerializeOverrides: operatorSerializeOverrides,
		AliasQuoteChar:             '"',
		IdentifierQuoteChar:        '"',
		ArgumentPlaceholder: func(ord int) string {
			return "$" + strconv.Itoa(ord)
		},
	}

	return jet.NewDialect(dialectParams)
}

func postgresCAST(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
			panic("jet: invalid number of expressions for operator")
		}

		expression := expressions[0]

		litExpr, ok := expressions[1].(jet.LiteralExpression)

		if !ok {
			panic("jet: cast invalid cast type")
		}

		castType, ok := litExpr.Value().(string)

		if !ok {
			panic("jet: cast type is not string")
		}

		jet.Serialize(expression, statement, out, options...)
		out.WriteString("::" + castType)
	}
}

func postgres_REGEXP_LIKE_operator(expressions ...jet.Expression) jet.SerializeFunc {
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

		if caseSensitive {
			out.WriteString("~")
		} else {
			out.WriteString("~*")
		}

		jet.Serialize(expressions[1], statement, out, options...)
	}
}

func postgres_NOT_REGEXP_LIKE_operator(expressions ...jet.Expression) jet.SerializeFunc {
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

		if caseSensitive {
			out.WriteString("!~")
		} else {
			out.WriteString("!~*")
		}

		jet.Serialize(expressions[1], statement, out, options...)
	}
}
