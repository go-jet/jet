package mysql

import (
	"github.com/go-jet/jet/internal/jet"
)

// Dialect is implementation of MySQL dialect for SQL Builder serialisation.
var Dialect = newDialect()

func newDialect() jet.Dialect {

	operatorSerializeOverrides := map[string]jet.SerializeOverride{}
	operatorSerializeOverrides[jet.StringRegexpLikeOperator] = mysqlREGEXPLIKEoperator
	operatorSerializeOverrides[jet.StringNotRegexpLikeOperator] = mysqlNOTREGEXPLIKEoperator
	operatorSerializeOverrides["IS DISTINCT FROM"] = mysqlISDISTINCTFROM
	operatorSerializeOverrides["IS NOT DISTINCT FROM"] = mysqlISNOTDISTINCTFROM
	operatorSerializeOverrides["/"] = mysqlDivision
	operatorSerializeOverrides["#"] = mysqlBitXor
	operatorSerializeOverrides[jet.StringConcatOperator] = mysqlCONCAToperator

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

func mysqlBitXor(expressions ...jet.Expression) jet.SerializeFunc {
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

func mysqlCONCAToperator(expressions ...jet.Expression) jet.SerializeFunc {
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

func mysqlDivision(expressions ...jet.Expression) jet.SerializeFunc {
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

func mysqlISNOTDISTINCTFROM(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
			panic("jet: invalid number of expressions for operator")
		}

		jet.Serialize(expressions[0], statement, out)
		out.WriteString("<=>")
		jet.Serialize(expressions[1], statement, out)
	}
}

func mysqlISDISTINCTFROM(expressions ...jet.Expression) jet.SerializeFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		out.WriteString("NOT(")
		mysqlISNOTDISTINCTFROM(expressions...)(statement, out, options...)
		out.WriteString(")")
	}
}

func mysqlREGEXPLIKEoperator(expressions ...jet.Expression) jet.SerializeFunc {
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

func mysqlNOTREGEXPLIKEoperator(expressions ...jet.Expression) jet.SerializeFunc {
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
