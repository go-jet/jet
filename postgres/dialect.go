package postgres

import (
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/go-jet/jet/v2/internal/jet"
)

// Dialect is implementation of postgres dialect for SQL Builder serialisation.
var Dialect = newDialect()

func newDialect() jet.Dialect {

	dialectParams := jet.DialectParams{
		Name:                       "PostgreSQL",
		PackageName:                "postgres",
		OperatorSerializeOverrides: nil,
		AliasQuoteChar:             '"',
		IdentifierQuoteChar:        '"',
		ArgumentPlaceholder: func(ord int) string {
			return "$" + strconv.Itoa(ord)
		},
		ArgumentToString: argumentToString,
		ReservedWords:    reservedWords,
		ValuesDefaultColumnName: func(index int) string {
			return fmt.Sprintf("column%d", index+1)
		},
		JsonValueEncode: func(expr Expression) Expression {
			switch e := expr.(type) {
			case ByteaExpression:
				return ENCODE(e, Base64)

			// CustomExpression used bellow (instead TO_CHAR function) so that only expr is parametrized
			case TimeExpression:
				return CustomExpression(Token("'0000-01-01T' || to_char('2000-10-10'::date + "), e, Token(`, 'HH24:MI:SS.USZ')`))
			case TimezExpression:
				return CustomExpression(Token("'0000-01-01T' || to_char('2000-10-10'::date + "), e, Token(`, 'HH24:MI:SS.USTZH:TZM')`))
			case TimestampExpression:
				return jet.AtomicCustomExpression(Token("to_char("), e, Token(`, 'YYYY-MM-DD"T"HH24:MI:SS.USZ')`))
			case DateExpression:
				return CustomExpression(Token("to_char("), e, Token(`::timestamp, 'YYYY-MM-DD') || 'T00:00:00Z'`))
			}
			return expr
		},
		RegexpLike: regexpLike,
	}

	return jet.NewDialect(dialectParams)
}

func argumentToString(value any) (string, bool) {
	switch bindVal := value.(type) {
	case []byte:
		return fmt.Sprintf("'\\x%s'", hex.EncodeToString(bindVal)), true
	}

	return "", false
}

func regexpLike(str jet.StringExpression, not bool, pattern jet.StringExpression, caseSensitive bool) jet.SerializerFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		jet.Serialize(str, statement, out, options...)

		var notOperator string

		if not {
			notOperator = "!"
		}

		if caseSensitive {
			out.WriteString(notOperator + "~")
		} else {
			out.WriteString(notOperator + "~*")
		}

		jet.Serialize(pattern, statement, out, options...)
	}
}

var reservedWords = []string{
	"ALL",
	"ANALYSE",
	"ANALYZE",
	"AND",
	"ANY",
	"ARRAY",
	"AS",
	"ASC",
	"ASYMMETRIC",
	"AUTHORIZATION",
	"BINARY",
	"BOTH",
	"CASE",
	"CAST",
	"CHECK",
	"COLLATE",
	"COLLATION",
	"COLUMN",
	"CONCURRENTLY",
	"CONSTRAINT",
	"CREATE",
	"CROSS",
	"CURRENT_CATALOG",
	"CURRENT_DATE",
	"CURRENT_ROLE",
	"CURRENT_SCHEMA",
	"CURRENT_TIME",
	"CURRENT_TIMESTAMP",
	"CURRENT_USER",
	"DEFAULT",
	"DEFERRABLE",
	"DESC",
	"DISTINCT",
	"DO",
	"ELSE",
	"END",
	"EXCEPT",
	"FALSE",
	"FETCH",
	"FOR",
	"FOREIGN",
	"FREEZE",
	"FROM",
	"FULL",
	"GRANT",
	"GROUP",
	"HAVING",
	"ILIKE",
	"IN",
	"INITIALLY",
	"INNER",
	"INTERSECT",
	"INTO",
	"IS",
	"ISNULL",
	"JOIN",
	"LATERAL",
	"LEADING",
	"LEFT",
	"LIKE",
	"LIMIT",
	"LOCALTIME",
	"LOCALTIMESTAMP",
	"NATURAL",
	"NOT",
	"NOTNULL",
	"NULL",
	"OFFSET",
	"ON",
	"ONLY",
	"OR",
	"ORDER",
	"OUTER",
	"OVERLAPS",
	"PLACING",
	"PRIMARY",
	"REFERENCES",
	"RETURNING",
	"RIGHT",
	"SELECT",
	"SESSION_USER",
	"SIMILAR",
	"SOME",
	"SYMMETRIC",
	"SYSTEM_USER",
	"TABLE",
	"TABLESAMPLE",
	"THEN",
	"TO",
	"TRAILING",
	"TRUE",
	"UNION",
	"UNIQUE",
	"USER",
	"USING",
	"VARIADIC",
	"VERBOSE",
	"WHEN",
	"WHERE",
	"WINDOW",
	"WITH",
}
