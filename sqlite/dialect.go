package sqlite

import (
	"fmt"
	"github.com/go-jet/jet/v2/internal/jet"
)

// Dialect is implementation of SQL Builder for SQLite databases.
var Dialect = newDialect()

func newDialect() jet.Dialect {
	operatorSerializeOverrides := map[string]jet.SerializeOverride{}
	operatorSerializeOverrides["IS DISTINCT FROM"] = sqlite_IS_DISTINCT_FROM
	operatorSerializeOverrides["IS NOT DISTINCT FROM"] = sqlite_IS_NOT_DISTINCT_FROM
	operatorSerializeOverrides["#"] = sqliteBitXOR

	mySQLDialectParams := jet.DialectParams{
		Name:                       "SQLite",
		PackageName:                "sqlite",
		OperatorSerializeOverrides: operatorSerializeOverrides,
		AliasQuoteChar:             '"',
		IdentifierQuoteChar:        '`',
		ArgumentPlaceholder: func(int) string {
			return "?"
		},
		ReservedWords: reservedWords2,
		ValuesDefaultColumnName: func(index int) string {
			return fmt.Sprintf("column%d", index+1)
		},
	}

	return jet.NewDialect(mySQLDialectParams)
}

func sqliteBitXOR(expressions ...jet.Serializer) jet.SerializerFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
			panic("jet: invalid number of expressions for operator XOR")
		}

		// (~(a&b))&(a|b)
		a := expressions[0]
		b := expressions[1]

		out.WriteString("(~(")
		jet.Serialize(a, statement, out, options...)
		out.WriteByte('&')
		jet.Serialize(b, statement, out, options...)
		out.WriteString("))&(")
		jet.Serialize(a, statement, out, options...)
		out.WriteByte('|')
		jet.Serialize(b, statement, out, options...)
		out.WriteByte(')')
	}
}

func sqlite_IS_NOT_DISTINCT_FROM(expressions ...jet.Serializer) jet.SerializerFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
			panic("jet: invalid number of expressions for operator")
		}

		jet.Serialize(expressions[0], statement, out)
		out.WriteString("IS")
		jet.Serialize(expressions[1], statement, out)
	}
}

func sqlite_IS_DISTINCT_FROM(expressions ...jet.Serializer) jet.SerializerFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
			panic("jet: invalid number of expressions for operator")
		}

		jet.Serialize(expressions[0], statement, out)
		out.WriteString("IS NOT")
		jet.Serialize(expressions[1], statement, out)
	}
}

var reservedWords2 = []string{
	"ABORT",
	"ACTION",
	"ADD",
	"AFTER",
	"ALL",
	"ALTER",
	"ALWAYS",
	"ANALYZE",
	"AND",
	"AS",
	"ASC",
	"ATTACH",
	"AUTOINCREMENT",
	"BEFORE",
	"BEGIN",
	"BETWEEN",
	"BY",
	"CASCADE",
	"CASE",
	"CAST",
	"CHECK",
	"COLLATE",
	"COLUMN",
	"COMMIT",
	"CONFLICT",
	"CONSTRAINT",
	"CREATE",
	"CROSS",
	"CURRENT",
	"CURRENT_DATE",
	"CURRENT_TIME",
	"CURRENT_TIMESTAMP",
	"DATABASE",
	"DEFAULT",
	"DEFERRABLE",
	"DEFERRED",
	"DELETE",
	"DESC",
	"DETACH",
	"DISTINCT",
	"DO",
	"DROP",
	"EACH",
	"ELSE",
	"END",
	"ESCAPE",
	"EXCEPT",
	"EXCLUDE",
	"EXCLUSIVE",
	"EXISTS",
	"EXPLAIN",
	"FAIL",
	"FILTER",
	"FIRST",
	"FOLLOWING",
	"FOR",
	"FOREIGN",
	"FROM",
	"FULL",
	"GENERATED",
	"GLOB",
	"GROUP",
	"GROUPS",
	"HAVING",
	"IF",
	"IGNORE",
	"IMMEDIATE",
	"IN",
	"INDEX",
	"INDEXED",
	"INITIALLY",
	"INNER",
	"INSERT",
	"INSTEAD",
	"INTERSECT",
	"INTO",
	"IS",
	"ISNULL",
	"JOIN",
	"KEY",
	"LAST",
	"LEFT",
	"LIKE",
	"LIMIT",
	"MATCH",
	"MATERIALIZED",
	"NATURAL",
	"NO",
	"NOT",
	"NOTHING",
	"NOTNULL",
	"NULL",
	"NULLS",
	"OF",
	"OFFSET",
	"ON",
	"OR",
	"ORDER",
	"OTHERS",
	"OUTER",
	"OVER",
	"PARTITION",
	"PLAN",
	"PRAGMA",
	"PRECEDING",
	"PRIMARY",
	"QUERY",
	"RAISE",
	"RANGE",
	"RECURSIVE",
	"REFERENCES",
	"REGEXP",
	"REINDEX",
	"RELEASE",
	"RENAME",
	"REPLACE",
	"RESTRICT",
	"RETURNING",
	"RIGHT",
	"ROLLBACK",
	"ROW",
	"ROWS",
	"SAVEPOINT",
	"SELECT",
	"SET",
	"TABLE",
	"TEMP",
	"TEMPORARY",
	"THEN",
	"TIES",
	"TO",
	"TRANSACTION",
	"TRIGGER",
	"UNBOUNDED",
	"UNION",
	"UNIQUE",
	"UPDATE",
	"USING",
	"VACUUM",
	"VALUES",
	"VIEW",
	"VIRTUAL",
	"WHEN",
	"WHERE",
	"WINDOW",
	"WITH",
	"WITHOUT",
}
