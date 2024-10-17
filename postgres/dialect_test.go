package postgres

import "testing"

func TestString_REGEXP_LIKE_operator(t *testing.T) {
	assertSerialize(t, table3StrCol.REGEXP_LIKE(table2ColStr), "(table3.col2 ~* table2.col_str)")
	assertSerialize(t, table3StrCol.REGEXP_LIKE(String("JOHN")), "(table3.col2 ~* $1::text)", "JOHN")
	assertSerialize(t, table3StrCol.REGEXP_LIKE(String("JOHN"), false), "(table3.col2 ~* $1::text)", "JOHN")
	assertSerialize(t, table3StrCol.REGEXP_LIKE(String("JOHN"), true), "(table3.col2 ~ $1::text)", "JOHN")
}

func TestString_NOT_REGEXP_LIKE_operator(t *testing.T) {
	assertSerialize(t, table3StrCol.NOT_REGEXP_LIKE(table2ColStr), "(table3.col2 !~* table2.col_str)")
	assertSerialize(t, table3StrCol.NOT_REGEXP_LIKE(String("JOHN")), "(table3.col2 !~* $1::text)", "JOHN")
	assertSerialize(t, table3StrCol.NOT_REGEXP_LIKE(String("JOHN"), false), "(table3.col2 !~* $1::text)", "JOHN")
	assertSerialize(t, table3StrCol.NOT_REGEXP_LIKE(String("JOHN"), true), "(table3.col2 !~ $1::text)", "JOHN")
}

func TestExists(t *testing.T) {
	assertSerialize(t, EXISTS(
		table2.
			SELECT(Int(1)).
			WHERE(table1Col1.EQ(table2Col3)),
	),
		`(EXISTS (
     SELECT $1
     FROM db.table2
     WHERE table1.col1 = table2.col3
))`, int64(1))

	assertSerialize(t, EXISTS(
		SELECT(Int(1)),
	).EQ(Bool(true)),
		`((EXISTS (
     SELECT $1
)) = $2::boolean)`, int64(1), true)

	assertProjectionSerialize(t, EXISTS(
		SELECT(Int(1)),
	).AS("exists"),
		`(EXISTS (
     SELECT $1
)) AS "exists"`, int64(1))
}

func TestIN(t *testing.T) {

	assertSerialize(t, Float(1.11).IN(table1.SELECT(table1Col1)),
		`($1 IN ((
     SELECT table1.col1 AS "table1.col1"
     FROM db.table1
)))`, float64(1.11))

	assertSerialize(t, ROW(Int(12), table1Col1).IN(table2.SELECT(table2Col3, table3Col1)),
		`(ROW($1, table1.col1) IN ((
     SELECT table2.col3 AS "table2.col3",
          table3.col1 AS "table3.col1"
     FROM db.table2
)))`, int64(12))
}

func TestNOT_IN(t *testing.T) {

	assertSerialize(t, Float(1.11).NOT_IN(table1.SELECT(table1Col1)),
		`($1 NOT IN ((
     SELECT table1.col1 AS "table1.col1"
     FROM db.table1
)))`, float64(1.11))

	assertSerialize(t, ROW(Int(12), table1Col1).NOT_IN(table2.SELECT(table2Col3, table3Col1)),
		`(ROW($1, table1.col1) NOT IN ((
     SELECT table2.col3 AS "table2.col3",
          table3.col1 AS "table3.col1"
     FROM db.table2
)))`, int64(12))
}

func TestReservedWordEscaped(t *testing.T) {
	var table1ColUser = IntervalColumn("user")
	var table1ColVariadic = IntervalColumn("VARIADIC")
	var table1ColProcedure = IntervalColumn("procedure")

	_ = NewTable("db", "table1", "", table1ColUser, table1ColVariadic, table1ColProcedure)

	assertSerialize(t, table1ColUser, `table1."user"`)
	assertSerialize(t, table1ColVariadic, `table1."VARIADIC"`)
	assertSerialize(t, table1ColProcedure, `table1.procedure`)
}
