package postgres

import (
	"errors"
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
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) error {
		if len(expressions) < 2 {
			return errors.New("jet: invalid number of expressions for operator")
		}

		expression := expressions[0]

		litExpr, ok := expressions[1].(jet.LiteralExpression)

		if !ok {
			return errors.New("jet: cast invalid cast type")
		}

		castType, ok := litExpr.Value().(string)

		if !ok {
			return errors.New("jet: cast type is not string")
		}

		if err := jet.Serialize(expression, statement, out, options...); err != nil {
			return err
		}
		out.WriteString("::" + castType)
		return nil
	}
}

func postgres_REGEXP_LIKE_function(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SqlBuilder, options ...jet.SerializeOption) error {
		if len(expressions) < 2 {
			return errors.New("jet: invalid number of expressions for operator")
		}

		if err := jet.Serialize(expressions[0], statement, out, options...); err != nil {
			return err
		}

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

		if err := jet.Serialize(expressions[1], statement, out, options...); err != nil {
			return err
		}

		return nil
	}
}
