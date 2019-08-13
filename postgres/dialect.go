package postgres

import (
	"github.com/go-jet/jet/internal/jet"
	"strconv"
	"strings"
)

var Dialect = NewDialect()

func NewDialect() jet.Dialect {

	serializeOverrides := map[string]jet.SerializeOverride{}
	serializeOverrides["REGEXP_LIKE"] = postgres_REGEXP_LIKE_function
	serializeOverrides["CAST"] = postgresCAST

	dialectParams := jet.DialectParams{
		Name:                "PostgreSQL",
		PackageName:         "postgres",
		SerializeOverrides:  serializeOverrides,
		AliasQuoteChar:      '"',
		IdentifierQuoteChar: '"',
		ArgumentPlaceholder: func(ord int) string {
			return "$" + strconv.Itoa(ord)
		},
	}

	return jet.NewDialect(dialectParams)
}

func postgresCAST(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) {
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

func postgres_REGEXP_LIKE_function(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
			panic("jet: invalid number of expressions for operator")
		}

		jet.Serialize(expressions[0], statement, out, options...)

		caseSensitive := false

		if len(expressions) >= 3 {
			if stringLiteral, ok := expressions[2].(jet.LiteralExpression); ok {
				matchType := stringLiteral.Value().(string)

				caseSensitive = !strings.Contains(matchType, "i")
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
