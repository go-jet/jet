package mysql

import (
	"fmt"
	"github.com/go-jet/jet/v2/internal/jet"
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
		ReservedWords:    reservedWords,
		SerializeOrderBy: serializeOrderBy,
		ValuesDefaultColumnName: func(index int) string {
			return fmt.Sprintf("column_%d", index)
		},
	}

	return jet.NewDialect(mySQLDialectParams)
}

func mysqlBitXor(expressions ...jet.Serializer) jet.SerializerFunc {
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

func mysqlCONCAToperator(expressions ...jet.Serializer) jet.SerializerFunc {
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

func mysqlDivision(expressions ...jet.Serializer) jet.SerializerFunc {
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

func mysqlISNOTDISTINCTFROM(expressions ...jet.Serializer) jet.SerializerFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		if len(expressions) < 2 {
			panic("jet: invalid number of expressions for operator")
		}

		jet.Serialize(expressions[0], statement, out)
		out.WriteString("<=>")
		jet.Serialize(expressions[1], statement, out)
	}
}

func mysqlISDISTINCTFROM(expressions ...jet.Serializer) jet.SerializerFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
		out.WriteString("NOT(")
		mysqlISNOTDISTINCTFROM(expressions...)(statement, out, options...)
		out.WriteString(")")
	}
}

func mysqlREGEXPLIKEoperator(expressions ...jet.Serializer) jet.SerializerFunc {
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

func mysqlNOTREGEXPLIKEoperator(expressions ...jet.Serializer) jet.SerializerFunc {
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

func serializeOrderBy(expression Expression, ascending, nullsFirst *bool) jet.SerializerFunc {
	return func(statement jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {

		if nullsFirst == nil {
			jet.SerializeForOrderBy(expression, statement, out)

			if ascending != nil {
				serializeAscending(*ascending, out)
			}
			return
		}

		asc := true

		if ascending != nil {
			asc = *ascending
		}

		if asc {
			if !*nullsFirst {
				jet.SerializeForOrderBy(expression.IS_NULL(), statement, out)
				out.WriteString(", ")
			}
			jet.SerializeForOrderBy(expression, statement, out)
			if ascending != nil {
				serializeAscending(asc, out)
			}
		} else {
			if *nullsFirst {
				jet.SerializeForOrderBy(expression.IS_NOT_NULL(), statement, out)
				out.WriteString(", ")
			}

			jet.SerializeForOrderBy(expression, statement, out)
			serializeAscending(asc, out)
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
	"ACCESSIBLE",
	"ADD",
	"ALL",
	"ALTER",
	"ANALYZE",
	"AND",
	"AS",
	"ASC",
	"ASENSITIVE",
	"BEFORE",
	"BETWEEN",
	"BIGINT",
	"BINARY",
	"BLOB",
	"BOTH",
	"BY",
	"CALL",
	"CASCADE",
	"CASE",
	"CHANGE",
	"CHAR",
	"CHARACTER",
	"CHECK",
	"COLLATE",
	"COLUMN",
	"CONDITION",
	"CONSTRAINT",
	"CONTINUE",
	"CONVERT",
	"CREATE",
	"CROSS",
	"CUBE",
	"CUME_DIST",
	"CURRENT_DATE",
	"CURRENT_TIME",
	"CURRENT_TIMESTAMP",
	"CURRENT_USER",
	"CURSOR",
	"DATABASE",
	"DATABASES",
	"DAY_HOUR",
	"DAY_MICROSECOND",
	"DAY_MINUTE",
	"DAY_SECOND",
	"DEC",
	"DECIMAL",
	"DECLARE",
	"DEFAULT",
	"DELAYED",
	"DELETE",
	"DENSE_RANK",
	"DESC",
	"DESCRIBE",
	"DETERMINISTIC",
	"DISTINCT",
	"DISTINCTROW",
	"DIV",
	"DOUBLE",
	"DROP",
	"DUAL",
	"EACH",
	"ELSE",
	"ELSEIF",
	"EMPTY",
	"ENCLOSED",
	"ESCAPED",
	"EXCEPT",
	"EXISTS",
	"EXIT",
	"EXPLAIN",
	"FALSE",
	"FETCH",
	"FIRST_VALUE",
	"FLOAT",
	"FLOAT4",
	"FLOAT8",
	"FOR",
	"FORCE",
	"FOREIGN",
	"FROM",
	"FULLTEXT",
	"FUNCTION",
	"GENERATED",
	"GET",
	"GRANT",
	"GROUP",
	"GROUPING",
	"GROUPS",
	"HAVING",
	"HIGH_PRIORITY",
	"HOUR_MICROSECOND",
	"HOUR_MINUTE",
	"HOUR_SECOND",
	"IF",
	"IGNORE",
	"IN",
	"INDEX",
	"INFILE",
	"INNER",
	"INOUT",
	"INSENSITIVE",
	"INSERT",
	"INT",
	"INT1",
	"INT2",
	"INT3",
	"INT4",
	"INT8",
	"INTEGER",
	"INTERVAL",
	"INTO",
	"IO_AFTER_GTIDS",
	"IO_BEFORE_GTIDS",
	"IS",
	"ITERATE",
	"JOIN",
	"JSON_TABLE",
	"KEY",
	"KEYS",
	"KILL",
	"LAG",
	"LAST_VALUE",
	"LATERAL",
	"LEAD",
	"LEADING",
	"LEAVE",
	"LEFT",
	"LIKE",
	"LIMIT",
	"LINEAR",
	"LINES",
	"LOAD",
	"LOCALTIME",
	"LOCALTIMESTAMP",
	"LOCK",
	"LONG",
	"LONGBLOB",
	"LONGTEXT",
	"LOOP",
	"LOW_PRIORITY",
	"MASTER_BIND",
	"MASTER_SSL_VERIFY_SERVER_CERT",
	"MATCH",
	"MAXVALUE",
	"MEDIUMBLOB",
	"MEDIUMINT",
	"MEDIUMTEXT",
	"MIDDLEINT",
	"MINUTE_MICROSECOND",
	"MINUTE_SECOND",
	"MOD",
	"MODIFIES",
	"NATURAL",
	"NOT",
	"NO_WRITE_TO_BINLOG",
	"NTH_VALUE",
	"NTILE",
	"NULL",
	"NUMERIC",
	"OF",
	"ON",
	"OPTIMIZE",
	"OPTIMIZER_COSTS",
	"OPTION",
	"OPTIONALLY",
	"OR",
	"ORDER",
	"OUT",
	"OUTER",
	"OUTFILE",
	"OVER",
	"PARTITION",
	"PERCENT_RANK",
	"PRECISION",
	"PRIMARY",
	"PROCEDURE",
	"PURGE",
	"RANGE",
	"RANK",
	"READ",
	"READS",
	"READ_WRITE",
	"REAL",
	"RECURSIVE",
	"REFERENCES",
	"REGEXP",
	"RELEASE",
	"RENAME",
	"REPEAT",
	"REPLACE",
	"REQUIRE",
	"RESIGNAL",
	"RESTRICT",
	"RETURN",
	"REVOKE",
	"RIGHT",
	"RLIKE",
	"ROW",
	"ROWS",
	"ROW_NUMBER",
	"SCHEMA",
	"SCHEMAS",
	"SECOND_MICROSECOND",
	"SELECT",
	"SENSITIVE",
	"SEPARATOR",
	"SET",
	"SHOW",
	"SIGNAL",
	"SMALLINT",
	"SPATIAL",
	"SPECIFIC",
	"SQL",
	"SQLEXCEPTION",
	"SQLSTATE",
	"SQLWARNING",
	"SQL_BIG_RESULT",
	"SQL_CALC_FOUND_ROWS",
	"SQL_SMALL_RESULT",
	"SSL",
	"STARTING",
	"STORED",
	"STRAIGHT_JOIN",
	"SYSTEM",
	"TABLE",
	"TERMINATED",
	"THEN",
	"TINYBLOB",
	"TINYINT",
	"TINYTEXT",
	"TO",
	"TRAILING",
	"TRIGGER",
	"TRUE",
	"UNDO",
	"UNION",
	"UNIQUE",
	"UNLOCK",
	"UNSIGNED",
	"UPDATE",
	"USAGE",
	"USE",
	"USING",
	"UTC_DATE",
	"UTC_TIME",
	"UTC_TIMESTAMP",
	"VALUES",
	"VARBINARY",
	"VARCHAR",
	"VARCHARACTER",
	"VARYING",
	"VIRTUAL",
	"WHEN",
	"WHERE",
	"WHILE",
	"WINDOW",
	"WITH",
	"WRITE",
	"XOR",
	"YEAR_MONTH",
	"ZEROFILL",
}
