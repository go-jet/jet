package postgres

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRaw(t *testing.T) {
	assertSerialize(t, Raw("current_database()"), "(current_database())")
	assertDebugSerialize(t, Raw("current_database()"), "(current_database())")

	assertSerialize(t, Raw(":first_arg + table.colInt + :second_arg", RawArgs{":first_arg": 11, ":second_arg": 22}),
		"($1 + table.colInt + $2)", 11, 22)
	assertDebugSerialize(t, Raw(":first_arg + table.colInt + :second_arg", RawArgs{":first_arg": 11, ":second_arg": 22}),
		"(11 + table.colInt + 22)")

	assertSerialize(t,
		Int(700).ADD(RawInt(":first_arg + table.colInt + :second_arg", RawArgs{":first_arg": 11, ":second_arg": 22})),
		"($1 + ($2 + table.colInt + $3))",
		int64(700), 11, 22)
	assertDebugSerialize(t,
		Int(700).ADD(RawInt(":first_arg + table.colInt + :second_arg", RawArgs{":first_arg": 11, ":second_arg": 22})),
		"(700 + (11 + table.colInt + 22))")
}

func TestDuplicateArguments(t *testing.T) {
	assertSerialize(t, Raw(":arg + table.colInt + :arg", RawArgs{":arg": 11}),
		"($1 + table.colInt + $1)", 11)
	assertDebugSerialize(t, Raw(":arg + table.colInt + :arg", RawArgs{":arg": 11}),
		"(11 + table.colInt + 11)")

	assertSerialize(t, Raw("#age + table.colInt + #year + #age + #year + 11", RawArgs{"#age": 11, "#year": 2000}),
		"($1 + table.colInt + $2 + $1 + $2 + 11)", 11, 2000)
	assertDebugSerialize(t, Raw("#age + table.colInt + #year + #age + #year + 11", RawArgs{"#age": 11, "#year": 2000}),
		"(11 + table.colInt + 2000 + 11 + 2000 + 11)")

	assertSerialize(t, Raw("#1 + all_types.integer + #2 + #1 + #2 + #3 + #4",
		RawArgs{"#1": 11, "#2": 22, "#3": 33, "#4": 44}),
		`($1 + all_types.integer + $2 + $1 + $2 + $3 + $4)`, 11, 22, 33, 44)
	assertDebugSerialize(t, Raw("#1 + all_types.integer + #2 + #1 + #2 + #3 + #4",
		RawArgs{"#1": 11, "#2": 22, "#3": 33, "#4": 44}),
		`(11 + all_types.integer + 22 + 11 + 22 + 33 + 44)`)
}

func TestRawInvalidArguments(t *testing.T) {
	defer func() {
		r := recover()
		require.Equal(t, "jet: named argument 'first_arg' does not appear in raw query", r)
	}()

	assertSerialize(t, Raw("table.colInt + :second_arg", RawArgs{
		"first_arg":  11,
		"second_arg": 22,
	}), "(table.colInt + $1)", 22)
}

func TestRawHelperMethods(t *testing.T) {
	assertSerialize(t, RawBool("table.colInt < :float", RawArgs{":float": 11.22}).IS_FALSE(),
		"((table.colInt < $1) IS FALSE)", 11.22)

	assertSerialize(t, RawFloat("table.colInt + :float", RawArgs{":float": 11.22}).EQ(Float(3.14)),
		"((table.colInt + $1) = $2)", 11.22, 3.14)
	assertSerialize(t, RawString("table.colStr || str", RawArgs{"str": "doe"}).EQ(String("john doe")),
		"((table.colStr || $1) = $2::text)", "doe", "john doe")

	now := time.Now()
	assertSerialize(t, RawTime("table.colTime").EQ(TimeT(now)),
		"((table.colTime) = $1::time without time zone)", now)
	assertSerialize(t, RawTimez("table.colTime").EQ(TimezT(now)),
		"((table.colTime) = $1::time with time zone)", now)
	assertSerialize(t, RawTimestamp("table.colTimestamp").EQ(TimestampT(now)),
		"((table.colTimestamp) = $1::timestamp without time zone)", now)
	assertSerialize(t, RawTimestampz("table.colTimestampz").EQ(TimestampzT(now)),
		"((table.colTimestampz) = $1::timestamp with time zone)", now)
	assertSerialize(t, RawDate("table.colDate").EQ(DateT(now)),
		"((table.colDate) = $1::date)", now)
}

func TestSerializer_CustomExpressionDynamicArgs(t *testing.T) {
	JSONField := func(exp StringExpression, fields ...string) Expression {
		args := []Serializer{exp}

		for i, field := range fields {
			op := "->"
			if i == len(fields)-1 {
				op = "->>"
			}

			args = append(args, Token(op), String(field))
		}

		return CustomExpression(args...)
	}

	details := StringColumn("details")

	assertSerialize(t, JSONField(details, "address", "city"),
		"(details -> $1::text ->> $2::text)", "address", "city")
}

func TestTypeWrappersOnSharedColumnsConcurrently(t *testing.T) {
	newStatement := func() SelectStatement {
		return SELECT(
			BoolExp(table1ColBool),
			IntExp(table1ColInt).ADD(Int(1)),
			FloatExp(table1ColFloat).AS("float"),
			DateExp(table1ColDate),
			TimeExp(table1ColTime),
			TimezExp(table1ColTimez),
			TimestampExp(table1ColTimestamp),
			TimestampzExp(table1ColTimestampz),
			IntervalExp(table1ColInterval),
			Int8RangeExp(table1ColRange).IS_EMPTY(),
			ArrayExp[StringExpression](table1ColStringArray).AT(Int(1)),
			ByteaExp(table2ColStr),
		).FROM(
			table1.INNER_JOIN(table2, table1ColInt.EQ(table2ColInt)),
		).WHERE(
			StringExp(table2ColStr).EQ(String("x")),
		)
	}

	expectedQuery, expectedArgs := newStatement().Sql()

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 100; j++ {
				query, args := newStatement().Sql()

				assert.Equal(t, expectedQuery, query)
				assert.Equal(t, expectedArgs, args)
			}
		}()
	}

	wg.Wait()
}

func TestTypeWrappersOnColumnsInSelectJson(t *testing.T) {
	stmt := SELECT_JSON_OBJ(
		table1ColTimestamp,
		TimestampExp(table2ColStr).AS("colStr"),
		DateExp(table1ColTimestampz).AS("oneWrapper"),
		StringExp(TimestampExp(table2ColTimestampz)).AS("colTimestampz"),
		StringExp(TimestampExp(table1ColDate)).AS("twoWrappers"),
		TimestampExp(StringExp(DateExp(table2ColDate))).AS("colDate"),
		TimestampExp(StringExp(DateExp(table1ColTime))).AS("threeWrappers"),
	).FROM(
		table1.INNER_JOIN(table2, table1ColInt.EQ(table2ColInt)),
	)

	expectedSQL := `
SELECT row_to_json(records) AS "json"
FROM (
          SELECT to_char(table1.col_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "colTimestamp",
               to_char(table2.col_str, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "colStr",
               (to_char(table1.col_timestampz::timestamp, 'YYYY-MM-DD') || 'T00:00:00Z') AS "oneWrapper",
               table2.col_timestampz AS "colTimestampz",
               table1.col_date AS "twoWrappers",
               to_char(table2.col_date, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "colDate",
               to_char(table1.col_time, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "threeWrappers"
          FROM db.table1
               INNER JOIN db.table2 ON (table1.col_int = table2.col_int)
     ) AS records;
`
	assertDebugStatementSql(t, stmt, expectedSQL)

	// wrapping the same columns again does not change the statement
	for _, column := range []Expression{table1ColTimestamp, table2ColStr, table1ColTimestampz, table2ColTimestampz,
		table1ColDate, table2ColDate, table1ColTime} {
		StringExp(TimestampExp(column))
	}

	assertDebugStatementSql(t, stmt, expectedSQL)

	// only columns have default alias, wrapped column has to be aliased
	for _, projection := range []Projection{
		TimestampExp(table2ColStr),
		StringExp(TimestampExp(table2ColTimestampz)),
		TimestampExp(StringExp(DateExp(table2ColDate))),
	} {
		assertStatementSqlErr(t, SELECT_JSON_OBJ(projection).FROM(table2),
			"jet: expression need to be aliased when used as SELECT JSON projection.")
	}
}

func TestTypeWrappersAsProjection(t *testing.T) {
	// only columns have default alias, wrapped column is projected as any other expression
	assertDebugStatementSql(t,
		SELECT(
			table1ColInt,
			StringExp(table1ColInt),
			IntExp(StringExp(table1ColBool)),
			FloatExp(table1ColFloat).AS("float"),
			StringExp(table1ColDate).AS("table1.col_date"),
		).FROM(table1), `
SELECT table1.col_int AS "table1.col_int",
     table1.col_int,
     table1.col_bool,
     table1.col_float AS "float",
     table1.col_date AS "table1.col_date"
FROM db.table1;
`)

	// wrapped column has to be aliased to be exported from the sub-query
	subQuery := SELECT(
		StringExp(table1ColInt),
	).FROM(
		table1,
	).AsTable("sub_query")

	assertPanicErr(t, func() { subQuery.AllColumns() },
		"jet: can't export unaliased expression subQuery: sub_query, expression: table1.col_int")

	subQuery = SELECT(
		StringExp(table1ColInt).AS("table1.col_int"),
		FloatExp(table1ColFloat).AS("float"),
	).FROM(
		table1,
	).AsTable("sub_query")

	assertDebugStatementSql(t, SELECT(subQuery.AllColumns()).FROM(subQuery), `
SELECT sub_query."table1.col_int" AS "table1.col_int",
     sub_query.float AS "float"
FROM (
          SELECT table1.col_int AS "table1.col_int",
               table1.col_float AS "float"
          FROM db.table1
     ) AS sub_query;
`)

	// wrapped column is not referenced by the column default alias in the set statement order by
	assertDebugStatementSql(t,
		UNION(
			SELECT(table1ColInt).FROM(table1),
			SELECT(table2ColInt).FROM(table2),
		).ORDER_BY(
			table1ColInt,
			IntExp(table1ColInt).DESC(),
			StringExp(table1ColInt),
			IntegerColumn("table1.col_int").ASC(),
		), `
(
     SELECT table1.col_int AS "table1.col_int"
     FROM db.table1
)
UNION
(
     SELECT table2.col_int AS "table2.col_int"
     FROM db.table2
)
ORDER BY "table1.col_int", table1.col_int DESC, table1.col_int, "table1.col_int" ASC;
`)
}
