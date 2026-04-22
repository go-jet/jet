package cubrid

import (
	"encoding/hex"
	"fmt"

	"github.com/go-jet/jet/v2/internal/jet"
)

// Dialect is implementation of CUBRID dialect for SQL Builder serialization.
var Dialect = newDialect()

func newDialect() jet.Dialect {
	operatorSerializeOverrides := map[string]jet.SerializeOverride{}
	operatorSerializeOverrides["IS DISTINCT FROM"] = cubridISDISTINCTFROM
	operatorSerializeOverrides["IS NOT DISTINCT FROM"] = cubridISNOTDISTINCTFROM
	operatorSerializeOverrides["/"] = cubridDivision
	operatorSerializeOverrides[jet.StringConcatOperator] = cubridCONCAToperator

	cubridDialectParams := jet.DialectParams{
		Name:                       "CUBRID",
		PackageName:                "cubrid",
		OperatorSerializeOverrides: operatorSerializeOverrides,
		AliasQuoteChar:             '"',
		IdentifierQuoteChar:        '"',
		ArgumentPlaceholder: func(int) string {
			return "?"
		},
		ArgumentToString: argumentToString,
		ReservedWords:    reservedWords,
		SerializeOrderBy: serializeOrderBy,
		ValuesDefaultColumnName: func(index int) string {
			return fmt.Sprintf("column_%d", index)
		},
		JsonValueEncode: func(expr Expression) Expression {
			return expr
		},
	}

	return jet.NewDialect(cubridDialectParams)
}

func argumentToString(value any) (string, bool) {
	switch bindVal := value.(type) {
	case []byte:
		return fmt.Sprintf("X'%s'", hex.EncodeToString(bindVal)), true
	}
	return "", false
}

func cubridDivision(expressions ...jet.Serializer) jet.SerializerFunc {
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

func cubridCONCAToperator(expressions ...jet.Serializer) jet.SerializerFunc {
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

func cubridISNOTDISTINCTFROM(expressions ...jet.Serializer) jet.SerializerFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
			panic("jet: invalid number of expressions for operator")
		}
		// CUBRID does not support <=> or IS NOT DISTINCT FROM syntax.
		// Emulate: (a = b OR (a IS NULL AND b IS NULL))
		out.WriteString("(")
		jet.Serialize(expressions[0], statement, out)
		out.WriteString("=")
		jet.Serialize(expressions[1], statement, out)
		out.WriteString("OR (")
		jet.Serialize(expressions[0], statement, out)
		out.WriteString("IS NULL AND")
		jet.Serialize(expressions[1], statement, out)
		out.WriteString("IS NULL))")
	}
}

func cubridISDISTINCTFROM(expressions ...jet.Serializer) jet.SerializerFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		out.WriteString("NOT ")
		cubridISNOTDISTINCTFROM(expressions...)(statement, out, options...)
	}
}

func serializeOrderBy(expression Expression, ascending, nullsFirst *bool) jet.SerializerFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		jet.SerializeForOrderBy(expression, statement, out)
		if ascending != nil {
			serializeAscending(*ascending, out)
		}
		// CUBRID supports NULLS FIRST / NULLS LAST natively.
		if nullsFirst != nil {
			if *nullsFirst {
				out.WriteString("NULLS FIRST")
			} else {
				out.WriteString("NULLS LAST")
			}
		}
	}
}

func serializeAscending(ascending bool, out *jet.SQLBuilder) {
	if ascending {
		out.WriteString("ASC")
	} else {
		out.WriteString("DESC")
	}
}

var reservedWords = []string{
	"ADD", "ALL", "ALTER", "AND", "AS", "ASC",
	"BETWEEN", "BIGINT", "BIT", "BLOB", "BOOLEAN", "BY",
	"CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER",
	"CHECK", "CLASS", "CLOB", "COALESCE", "COLLATE", "COLUMN",
	"COMMIT", "CONSTRAINT", "CREATE", "CROSS",
	"CURRENT_DATE", "CURRENT_DATETIME", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
	"CURSOR", "DATABASE", "DATE", "DATETIME", "DATETIMETZ", "DATETIMELTZ",
	"DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DESC", "DESCRIBE",
	"DISTINCT", "DIV", "DOUBLE", "DROP",
	"EACH", "ELSE", "ELSEIF", "END", "ENUM", "ESCAPE",
	"EXCEPT", "EXCEPTION", "EXISTS", "EXPLAIN", "EXTRACT",
	"FALSE", "FETCH", "FLOAT", "FOR", "FOREIGN", "FROM", "FULL", "FUNCTION",
	"GRANT", "GROUP",
	"HAVING", "HOUR",
	"IF", "IN", "INDEX", "INHERIT", "INNER", "INOUT", "INSERT",
	"INT", "INTEGER", "INTERSECT", "INTO", "IS",
	"JOIN", "KEY",
	"LANGUAGE", "LEFT", "LEVEL", "LIKE", "LIMIT", "LIST", "LOCK",
	"MATCH", "MERGE", "METHOD", "MINUTE", "MODIFY", "MONETARY", "MONTH", "MULTISET",
	"NATIONAL", "NATURAL", "NCHAR", "NOT", "NULL", "NULLIF", "NUMERIC",
	"OBJECT", "OF", "ON", "ONLY", "OPEN", "OPTION", "OR", "ORDER", "OUT", "OUTER", "OVER",
	"PARTITION", "POSITION", "PRECISION", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE",
	"READ", "REAL", "RECURSIVE", "REFERENCES", "REGEXP", "RENAME", "REPLACE",
	"RESTRICT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLLBACK", "ROLLUP",
	"ROW", "ROWNUM", "ROWS",
	"SAVEPOINT", "SCHEMA", "SECOND", "SELECT", "SEQUENCE",
	"SERIAL", "SERIALIZABLE", "SESSION", "SET",
	"SHORT", "SMALLINT",
	"SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING",
	"STRING", "TABLE", "TEMPORARY", "THEN", "TIME", "TIMESTAMP",
	"TIMESTAMPTZ", "TIMESTAMPLTZ",
	"TO", "TRAILING", "TRANSACTION", "TRIGGER", "TRIM", "TRUE", "TRUNCATE",
	"UNDER", "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "USAGE", "USE", "USER", "USING",
	"VALUE", "VALUES", "VARCHAR", "VARYING", "VCLASS", "VIEW",
	"WHEN", "WHENEVER", "WHERE", "WHILE", "WITH", "WITHOUT", "WORK", "WRITE",
	"XOR", "YEAR", "ZONE",
}
