package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/google/uuid"

	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/view"
	"github.com/go-jet/jet/v2/tests/testdata/results/common"
)

func TestAllTypesSelect(t *testing.T) {
	skipForPgxDriver(t) // pgx driver returns time with time zone as string

	dest := []model.AllTypes{}

	err := AllTypes.SELECT(AllTypes.AllColumns).Query(db, &dest)
	require.NoError(t, err)

	testutils.AssertDeepEqual(t, dest[0], allTypesRow0)
	testutils.AssertDeepEqual(t, dest[1], allTypesRow1)
}

func TestAllTypesViewSelect(t *testing.T) {
	skipForPgxDriver(t) // pgx driver returns time with time zone as string

	type AllTypesView model.AllTypes

	dest := []AllTypesView{}

	err := view.AllTypesView.SELECT(view.AllTypesView.AllColumns).Query(db, &dest)
	require.NoError(t, err)

	testutils.AssertDeepEqual(t, dest[0], AllTypesView(allTypesRow0))
	testutils.AssertDeepEqual(t, dest[1], AllTypesView(allTypesRow1))
}

func TestAllTypesInsertModel(t *testing.T) {
	skipForPgxDriver(t) // pgx driver does not handle well time with time zone

	query := AllTypes.INSERT(AllTypes.AllColumns).
		MODEL(allTypesRow0).
		MODEL(&allTypesRow1).
		RETURNING(AllTypes.AllColumns)

	dest := []model.AllTypes{}
	err := query.Query(db, &dest)
	require.NoError(t, err)

	require.Equal(t, len(dest), 2)
	testutils.AssertDeepEqual(t, dest[0], allTypesRow0)
	testutils.AssertDeepEqual(t, dest[1], allTypesRow1)
}

func TestAllTypesInsertQuery(t *testing.T) {
	skipForPgxDriver(t) // pgx driver does not handle well time with time zone

	query := AllTypes.INSERT(AllTypes.AllColumns).
		QUERY(
			AllTypes.
				SELECT(AllTypes.AllColumns).
				LIMIT(2),
		).
		RETURNING(AllTypes.AllColumns)

	dest := []model.AllTypes{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 2)
	testutils.AssertDeepEqual(t, dest[0], allTypesRow0)
	testutils.AssertDeepEqual(t, dest[1], allTypesRow1)
}

func TestAllTypesFromSubQuery(t *testing.T) {
	skipForPgxDriver(t)

	subQuery := SELECT(AllTypes.AllColumns).
		FROM(AllTypes).
		AsTable("allTypesSubQuery")

	mainQuery := SELECT(subQuery.AllColumns()).
		FROM(subQuery).
		LIMIT(2)

	require.Equal(t, mainQuery.DebugSql(), `
SELECT "allTypesSubQuery"."all_types.small_int_ptr" AS "all_types.small_int_ptr",
     "allTypesSubQuery"."all_types.small_int" AS "all_types.small_int",
     "allTypesSubQuery"."all_types.integer_ptr" AS "all_types.integer_ptr",
     "allTypesSubQuery"."all_types.integer" AS "all_types.integer",
     "allTypesSubQuery"."all_types.big_int_ptr" AS "all_types.big_int_ptr",
     "allTypesSubQuery"."all_types.big_int" AS "all_types.big_int",
     "allTypesSubQuery"."all_types.decimal_ptr" AS "all_types.decimal_ptr",
     "allTypesSubQuery"."all_types.decimal" AS "all_types.decimal",
     "allTypesSubQuery"."all_types.numeric_ptr" AS "all_types.numeric_ptr",
     "allTypesSubQuery"."all_types.numeric" AS "all_types.numeric",
     "allTypesSubQuery"."all_types.real_ptr" AS "all_types.real_ptr",
     "allTypesSubQuery"."all_types.real" AS "all_types.real",
     "allTypesSubQuery"."all_types.double_precision_ptr" AS "all_types.double_precision_ptr",
     "allTypesSubQuery"."all_types.double_precision" AS "all_types.double_precision",
     "allTypesSubQuery"."all_types.smallserial" AS "all_types.smallserial",
     "allTypesSubQuery"."all_types.serial" AS "all_types.serial",
     "allTypesSubQuery"."all_types.bigserial" AS "all_types.bigserial",
     "allTypesSubQuery"."all_types.var_char_ptr" AS "all_types.var_char_ptr",
     "allTypesSubQuery"."all_types.var_char" AS "all_types.var_char",
     "allTypesSubQuery"."all_types.char_ptr" AS "all_types.char_ptr",
     "allTypesSubQuery"."all_types.char" AS "all_types.char",
     "allTypesSubQuery"."all_types.text_ptr" AS "all_types.text_ptr",
     "allTypesSubQuery"."all_types.text" AS "all_types.text",
     "allTypesSubQuery"."all_types.bytea_ptr" AS "all_types.bytea_ptr",
     "allTypesSubQuery"."all_types.bytea" AS "all_types.bytea",
     "allTypesSubQuery"."all_types.timestampz_ptr" AS "all_types.timestampz_ptr",
     "allTypesSubQuery"."all_types.timestampz" AS "all_types.timestampz",
     "allTypesSubQuery"."all_types.timestamp_ptr" AS "all_types.timestamp_ptr",
     "allTypesSubQuery"."all_types.timestamp" AS "all_types.timestamp",
     "allTypesSubQuery"."all_types.date_ptr" AS "all_types.date_ptr",
     "allTypesSubQuery"."all_types.date" AS "all_types.date",
     "allTypesSubQuery"."all_types.timez_ptr" AS "all_types.timez_ptr",
     "allTypesSubQuery"."all_types.timez" AS "all_types.timez",
     "allTypesSubQuery"."all_types.time_ptr" AS "all_types.time_ptr",
     "allTypesSubQuery"."all_types.time" AS "all_types.time",
     "allTypesSubQuery"."all_types.interval_ptr" AS "all_types.interval_ptr",
     "allTypesSubQuery"."all_types.interval" AS "all_types.interval",
     "allTypesSubQuery"."all_types.boolean_ptr" AS "all_types.boolean_ptr",
     "allTypesSubQuery"."all_types.boolean" AS "all_types.boolean",
     "allTypesSubQuery"."all_types.point_ptr" AS "all_types.point_ptr",
     "allTypesSubQuery"."all_types.bit_ptr" AS "all_types.bit_ptr",
     "allTypesSubQuery"."all_types.bit" AS "all_types.bit",
     "allTypesSubQuery"."all_types.bit_varying_ptr" AS "all_types.bit_varying_ptr",
     "allTypesSubQuery"."all_types.bit_varying" AS "all_types.bit_varying",
     "allTypesSubQuery"."all_types.tsvector_ptr" AS "all_types.tsvector_ptr",
     "allTypesSubQuery"."all_types.tsvector" AS "all_types.tsvector",
     "allTypesSubQuery"."all_types.uuid_ptr" AS "all_types.uuid_ptr",
     "allTypesSubQuery"."all_types.uuid" AS "all_types.uuid",
     "allTypesSubQuery"."all_types.xml_ptr" AS "all_types.xml_ptr",
     "allTypesSubQuery"."all_types.xml" AS "all_types.xml",
     "allTypesSubQuery"."all_types.json_ptr" AS "all_types.json_ptr",
     "allTypesSubQuery"."all_types.json" AS "all_types.json",
     "allTypesSubQuery"."all_types.jsonb_ptr" AS "all_types.jsonb_ptr",
     "allTypesSubQuery"."all_types.jsonb" AS "all_types.jsonb",
     "allTypesSubQuery"."all_types.integer_array_ptr" AS "all_types.integer_array_ptr",
     "allTypesSubQuery"."all_types.integer_array" AS "all_types.integer_array",
     "allTypesSubQuery"."all_types.text_array_ptr" AS "all_types.text_array_ptr",
     "allTypesSubQuery"."all_types.text_array" AS "all_types.text_array",
     "allTypesSubQuery"."all_types.jsonb_array" AS "all_types.jsonb_array",
     "allTypesSubQuery"."all_types.text_multi_dim_array_ptr" AS "all_types.text_multi_dim_array_ptr",
     "allTypesSubQuery"."all_types.text_multi_dim_array" AS "all_types.text_multi_dim_array"
FROM (
          SELECT all_types.small_int_ptr AS "all_types.small_int_ptr",
               all_types.small_int AS "all_types.small_int",
               all_types.integer_ptr AS "all_types.integer_ptr",
               all_types.integer AS "all_types.integer",
               all_types.big_int_ptr AS "all_types.big_int_ptr",
               all_types.big_int AS "all_types.big_int",
               all_types.decimal_ptr AS "all_types.decimal_ptr",
               all_types.decimal AS "all_types.decimal",
               all_types.numeric_ptr AS "all_types.numeric_ptr",
               all_types.numeric AS "all_types.numeric",
               all_types.real_ptr AS "all_types.real_ptr",
               all_types.real AS "all_types.real",
               all_types.double_precision_ptr AS "all_types.double_precision_ptr",
               all_types.double_precision AS "all_types.double_precision",
               all_types.smallserial AS "all_types.smallserial",
               all_types.serial AS "all_types.serial",
               all_types.bigserial AS "all_types.bigserial",
               all_types.var_char_ptr AS "all_types.var_char_ptr",
               all_types.var_char AS "all_types.var_char",
               all_types.char_ptr AS "all_types.char_ptr",
               all_types.char AS "all_types.char",
               all_types.text_ptr AS "all_types.text_ptr",
               all_types.text AS "all_types.text",
               all_types.bytea_ptr AS "all_types.bytea_ptr",
               all_types.bytea AS "all_types.bytea",
               all_types.timestampz_ptr AS "all_types.timestampz_ptr",
               all_types.timestampz AS "all_types.timestampz",
               all_types.timestamp_ptr AS "all_types.timestamp_ptr",
               all_types.timestamp AS "all_types.timestamp",
               all_types.date_ptr AS "all_types.date_ptr",
               all_types.date AS "all_types.date",
               all_types.timez_ptr AS "all_types.timez_ptr",
               all_types.timez AS "all_types.timez",
               all_types.time_ptr AS "all_types.time_ptr",
               all_types.time AS "all_types.time",
               all_types.interval_ptr AS "all_types.interval_ptr",
               all_types.interval AS "all_types.interval",
               all_types.boolean_ptr AS "all_types.boolean_ptr",
               all_types.boolean AS "all_types.boolean",
               all_types.point_ptr AS "all_types.point_ptr",
               all_types.bit_ptr AS "all_types.bit_ptr",
               all_types.bit AS "all_types.bit",
               all_types.bit_varying_ptr AS "all_types.bit_varying_ptr",
               all_types.bit_varying AS "all_types.bit_varying",
               all_types.tsvector_ptr AS "all_types.tsvector_ptr",
               all_types.tsvector AS "all_types.tsvector",
               all_types.uuid_ptr AS "all_types.uuid_ptr",
               all_types.uuid AS "all_types.uuid",
               all_types.xml_ptr AS "all_types.xml_ptr",
               all_types.xml AS "all_types.xml",
               all_types.json_ptr AS "all_types.json_ptr",
               all_types.json AS "all_types.json",
               all_types.jsonb_ptr AS "all_types.jsonb_ptr",
               all_types.jsonb AS "all_types.jsonb",
               all_types.integer_array_ptr AS "all_types.integer_array_ptr",
               all_types.integer_array AS "all_types.integer_array",
               all_types.text_array_ptr AS "all_types.text_array_ptr",
               all_types.text_array AS "all_types.text_array",
               all_types.jsonb_array AS "all_types.jsonb_array",
               all_types.text_multi_dim_array_ptr AS "all_types.text_multi_dim_array_ptr",
               all_types.text_multi_dim_array AS "all_types.text_multi_dim_array"
          FROM test_sample.all_types
     ) AS "allTypesSubQuery"
LIMIT 2;
`)

	dest := []model.AllTypes{}
	err := mainQuery.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 2)
}

func TestExpressionOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Integer.IS_NULL().AS("result.is_null"),
		AllTypes.DatePtr.IS_NOT_NULL().AS("result.is_not_null"),
		AllTypes.SmallIntPtr.IN(Int(11), Int(22)).AS("result.in"),
		AllTypes.SmallIntPtr.IN(AllTypes.SELECT(AllTypes.Integer)).AS("result.in_select"),

		Raw("CURRENT_USER").AS("result.raw"),
		Raw("#1 + COALESCE(all_types.small_int_ptr, 0) + #2", RawArgs{"#1": 78, "#2": 56}).AS("result.raw_arg"),
		Raw("#1 + all_types.integer + #2 + #1 + #3 + #4",
			RawArgs{"#1": 11, "#2": 22, "#3": 33, "#4": 44}).AS("result.raw_arg2"),

		AllTypes.SmallIntPtr.NOT_IN(Int(11), Int(22), NULL).AS("result.not_in"),
		AllTypes.SmallIntPtr.NOT_IN(AllTypes.SELECT(AllTypes.Integer)).AS("result.not_in_select"),
	).LIMIT(2)

	testutils.AssertStatementSql(t, query, `
SELECT all_types.integer IS NULL AS "result.is_null",
     all_types.date_ptr IS NOT NULL AS "result.is_not_null",
     (all_types.small_int_ptr IN ($1, $2)) AS "result.in",
     (all_types.small_int_ptr IN ((
          SELECT all_types.integer AS "all_types.integer"
          FROM test_sample.all_types
     ))) AS "result.in_select",
     (CURRENT_USER) AS "result.raw",
     ($3 + COALESCE(all_types.small_int_ptr, 0) + $4) AS "result.raw_arg",
     ($5 + all_types.integer + $6 + $5 + $7 + $8) AS "result.raw_arg2",
     (all_types.small_int_ptr NOT IN ($9, $10, NULL)) AS "result.not_in",
     (all_types.small_int_ptr NOT IN ((
          SELECT all_types.integer AS "all_types.integer"
          FROM test_sample.all_types
     ))) AS "result.not_in_select"
FROM test_sample.all_types
LIMIT $11;
`, int64(11), int64(22), 78, 56, 11, 22, 33, 44, int64(11), int64(22), int64(2))

	var dest []struct {
		common.ExpressionTestResult `alias:"result.*"`
	}

	err := query.Query(db, &dest)

	require.NoError(t, err)

	//testutils.PrintJson(dest)

	testutils.AssertJSON(t, dest, `
[
	{
		"IsNull": false,
		"IsNotNull": true,
		"In": false,
		"InSelect": false,
		"Raw": "jet",
		"RawArg": 148,
		"RawArg2": 421,
		"NotIn": null,
		"NotInSelect": true
	},
	{
		"IsNull": false,
		"IsNotNull": false,
		"In": null,
		"InSelect": null,
		"Raw": "jet",
		"RawArg": 134,
		"RawArg2": 421,
		"NotIn": null,
		"NotInSelect": null
	}
]
`)
}

func TestExpressionCast(t *testing.T) {

	skipForPgxDriver(t) // for some reason, pgx driver, 150:char(12) returns as int value

	query := AllTypes.SELECT(
		CAST(Int(150)).AS_CHAR(12).AS("char12"),
		CAST(String("TRUE")).AS_BOOL(),
		CAST(String("111")).AS_SMALLINT(),
		CAST(String("111")).AS_INTEGER(),
		CAST(String("111")).AS_BIGINT(),
		CAST(String("11.23")).AS_NUMERIC(30, 10),
		CAST(String("11.23")).AS_NUMERIC(30),
		CAST(String("11.23")).AS_NUMERIC(),
		CAST(String("11.23")).AS_REAL(),
		CAST(String("11.23")).AS_DOUBLE(),
		CAST(Int(234)).AS_TEXT(),
		CAST(String("1/8/1999")).AS_DATE(),
		CAST(String("04:05:06.789")).AS_TIME(),
		CAST(String("04:05:06 PST")).AS_TIMEZ(),
		CAST(String("1999-01-08 04:05:06")).AS_TIMESTAMP(),
		CAST(String("January 8 04:05:06 1999 PST")).AS_TIMESTAMPZ(),
		CAST(String("04:05:06")).AS_INTERVAL(),

		TO_CHAR(AllTypes.Timestamp, String("HH12:MI:SS")),
		TO_CHAR(AllTypes.Integer, String("999")),
		TO_CHAR(AllTypes.DoublePrecision, String("999D9")),
		TO_CHAR(AllTypes.Numeric, String("999D99S")),

		TO_DATE(String("05 Dec 2000"), String("DD Mon YYYY")),
		TO_NUMBER(String("12,454"), String("99G999D9S")),
		TO_TIMESTAMP(String("05 Dec 2000"), String("DD Mon YYYY")),

		COALESCE(AllTypes.IntegerPtr, AllTypes.SmallIntPtr, NULL, Int(11)),
		NULLIF(AllTypes.Text, String("(none)")),
		GREATEST(AllTypes.Numeric, AllTypes.NumericPtr),
		LEAST(AllTypes.Numeric, AllTypes.NumericPtr),

		Raw("current_database()"),
	)

	//fmt.Println(query.DebugSql())

	dest := []struct{}{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestStringOperators(t *testing.T) {
	skipForPgxDriver(t) // pgx driver returns text column as int value

	query := AllTypes.SELECT(
		AllTypes.Text.EQ(AllTypes.Char),
		AllTypes.Text.EQ(String("Text")),
		AllTypes.Text.NOT_EQ(AllTypes.VarCharPtr),
		AllTypes.Text.NOT_EQ(String("Text")),
		AllTypes.Text.GT(AllTypes.Text),
		AllTypes.Text.GT(String("Text")),
		AllTypes.Text.GT_EQ(AllTypes.TextPtr),
		AllTypes.Text.GT_EQ(String("Text")),
		AllTypes.Text.LT(AllTypes.Char),
		AllTypes.Text.LT(String("Text")),
		AllTypes.Text.LT_EQ(AllTypes.VarChar),
		AllTypes.Text.LT_EQ(String("Text")),
		AllTypes.Text.CONCAT(String("text2")),
		AllTypes.Text.CONCAT(Int(11)),
		AllTypes.Text.LIKE(String("abc")),
		AllTypes.Text.NOT_LIKE(String("_b_")),
		AllTypes.Text.REGEXP_LIKE(String("^t")),
		AllTypes.Text.REGEXP_LIKE(String("^t"), true),
		AllTypes.Text.NOT_REGEXP_LIKE(String("^t")),
		AllTypes.Text.NOT_REGEXP_LIKE(String("^t"), true),

		BIT_LENGTH(String("length")),
		CHAR_LENGTH(AllTypes.Char),
		CHAR_LENGTH(String("length")),
		OCTET_LENGTH(AllTypes.Text),
		OCTET_LENGTH(String("length")),
		LOWER(AllTypes.VarCharPtr),
		LOWER(String("length")),
		UPPER(AllTypes.Char),
		UPPER(String("upper")),
		BTRIM(AllTypes.VarChar),
		BTRIM(String("btrim")),
		BTRIM(AllTypes.VarChar, String("AA")),
		BTRIM(String("btrim"), String("AA")),
		LTRIM(AllTypes.VarChar),
		LTRIM(String("ltrim")),
		LTRIM(AllTypes.VarChar, String("A")),
		LTRIM(String("Ltrim"), String("A")),
		RTRIM(String("rtrim")),
		RTRIM(AllTypes.VarChar, String("B")),
		CHR(Int(65)),
		CONCAT(AllTypes.VarCharPtr, AllTypes.VarCharPtr, String("aaa"), Int(1)),
		CONCAT(Bool(false), Int(1), Float(22.2), String("test test")),
		CONCAT_WS(String("string1"), Int(1), Float(11.22), String("bytea"), Bool(false)), //Float(11.12)),
		CONVERT(String("bytea"), String("UTF8"), String("LATIN1")),
		CONVERT(AllTypes.Bytea, String("UTF8"), String("LATIN1")),
		CONVERT_FROM(String("text_in_utf8"), String("UTF8")),
		CONVERT_TO(String("text_in_utf8"), String("UTF8")),
		ENCODE(String("123\000\001"), String("base64")),
		DECODE(String("MTIzAAE="), String("base64")),
		FORMAT(String("Hello %s, %1$s"), String("World")),
		INITCAP(String("hi THOMAS")),
		LEFT(String("abcde"), Int(2)),
		RIGHT(String("abcde"), Int(2)),
		LENGTH(String("jose")),
		LENGTH(String("jose"), String("UTF8")),
		LPAD(String("Hi"), Int(5)),
		LPAD(String("Hi"), Int(5), String("xy")),
		RPAD(String("Hi"), Int(5)),
		RPAD(String("Hi"), Int(5), String("xy")),
		MD5(AllTypes.VarChar),
		REPEAT(AllTypes.Text, Int(33)),
		REPLACE(AllTypes.Char, String("BA"), String("AB")),
		REVERSE(AllTypes.VarChar),
		STRPOS(AllTypes.Text, String("A")),
		SUBSTR(AllTypes.Char, Int(3)),
		SUBSTR(AllTypes.CharPtr, Int(3), Int(2)),
		TO_HEX(AllTypes.IntegerPtr),
	)

	//fmt.Println(query.DebugSql())

	dest := []struct{}{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestBoolOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Boolean.EQ(AllTypes.BooleanPtr).AS("EQ1"),
		AllTypes.Boolean.EQ(Bool(true)).AS("EQ2"),
		AllTypes.Boolean.NOT_EQ(AllTypes.BooleanPtr).AS("NEq1"),
		AllTypes.Boolean.NOT_EQ(Bool(false)).AS("NEq2"),
		AllTypes.Boolean.IS_DISTINCT_FROM(AllTypes.BooleanPtr).AS("distinct1"),
		AllTypes.Boolean.IS_DISTINCT_FROM(Bool(true)).AS("distinct2"),
		AllTypes.Boolean.IS_NOT_DISTINCT_FROM(AllTypes.BooleanPtr).AS("not_distinct_1"),
		AllTypes.Boolean.IS_NOT_DISTINCT_FROM(Bool(true)).AS("NOTDISTINCT2"),
		AllTypes.Boolean.IS_TRUE().AS("ISTRUE"),
		AllTypes.Boolean.IS_NOT_TRUE().AS("isnottrue"),
		AllTypes.Boolean.IS_FALSE().AS("is_False"),
		AllTypes.Boolean.IS_NOT_FALSE().AS("is not false"),
		AllTypes.Boolean.IS_UNKNOWN().AS("is unknown"),
		AllTypes.Boolean.IS_NOT_UNKNOWN().AS("is_not_unknown"),

		AllTypes.Boolean.AND(AllTypes.Boolean).EQ(AllTypes.Boolean.AND(AllTypes.Boolean)).AS("complex1"),
		AllTypes.Boolean.OR(AllTypes.Boolean).EQ(AllTypes.Boolean.AND(AllTypes.Boolean)).AS("complex2"),
	).LIMIT(2)

	//fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, `
SELECT (all_types.boolean = all_types.boolean_ptr) AS "EQ1",
     (all_types.boolean = $1) AS "EQ2",
     (all_types.boolean != all_types.boolean_ptr) AS "NEq1",
     (all_types.boolean != $2) AS "NEq2",
     (all_types.boolean IS DISTINCT FROM all_types.boolean_ptr) AS "distinct1",
     (all_types.boolean IS DISTINCT FROM $3) AS "distinct2",
     (all_types.boolean IS NOT DISTINCT FROM all_types.boolean_ptr) AS "not_distinct_1",
     (all_types.boolean IS NOT DISTINCT FROM $4) AS "NOTDISTINCT2",
     all_types.boolean IS TRUE AS "ISTRUE",
     all_types.boolean IS NOT TRUE AS "isnottrue",
     all_types.boolean IS FALSE AS "is_False",
     all_types.boolean IS NOT FALSE AS "is not false",
     all_types.boolean IS UNKNOWN AS "is unknown",
     all_types.boolean IS NOT UNKNOWN AS "is_not_unknown",
     ((all_types.boolean AND all_types.boolean) = (all_types.boolean AND all_types.boolean)) AS "complex1",
     ((all_types.boolean OR all_types.boolean) = (all_types.boolean AND all_types.boolean)) AS "complex2"
FROM test_sample.all_types
LIMIT $5;
`, true, false, true, true, int64(2))

	var dest []struct {
		Eq1          *bool
		Eq2          *bool
		NEq1         *bool
		NEq2         *bool
		Distinct1    *bool
		Distinct2    *bool
		NotDistinct1 *bool
		NotDistinct2 *bool
		IsTrue       *bool
		IsNotTrue    *bool
		IsFalse      *bool
		IsNotFalse   *bool
		IsUnknown    *bool
		IsNotUnknown *bool

		Complex1 *bool
		Complex2 *bool
	}

	err := query.Query(db, &dest)

	require.NoError(t, err)

	testutils.AssertJSONFile(t, dest, "./testdata/results/common/bool_operators.json")
}

func TestFloatOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Numeric.EQ(AllTypes.Numeric).AS("eq1"),
		AllTypes.Decimal.EQ(Float(12.22)).AS("eq2"),
		AllTypes.Real.EQ(Float(12.12)).AS("eq3"),
		AllTypes.Numeric.IS_DISTINCT_FROM(AllTypes.Numeric).AS("distinct1"),
		AllTypes.Decimal.IS_DISTINCT_FROM(Float(12)).AS("distinct2"),
		AllTypes.Real.IS_DISTINCT_FROM(Float(12.12)).AS("distinct3"),
		AllTypes.Numeric.IS_NOT_DISTINCT_FROM(AllTypes.Numeric).AS("not_distinct1"),
		AllTypes.Decimal.IS_NOT_DISTINCT_FROM(Float(12)).AS("not_distinct2"),
		AllTypes.Real.IS_NOT_DISTINCT_FROM(Float(12.12)).AS("not_distinct3"),
		AllTypes.Numeric.LT(Float(124)).AS("lt1"),
		AllTypes.Numeric.LT(Float(34.56)).AS("lt2"),
		AllTypes.Numeric.GT(Float(124)).AS("gt1"),
		AllTypes.Numeric.GT(Float(34.56)).AS("gt2"),

		TRUNC(AllTypes.Decimal.ADD(AllTypes.Decimal), Int(2)).AS("add1"),
		TRUNC(AllTypes.Decimal.ADD(Float(11.22)), Int(2)).AS("add2"),
		TRUNC(AllTypes.Decimal.SUB(AllTypes.DecimalPtr), Int(2)).AS("sub1"),
		TRUNC(AllTypes.Decimal.SUB(Float(11.22)), Int(2)).AS("sub2"),
		TRUNC(AllTypes.Decimal.MUL(AllTypes.DecimalPtr), Int(2)).AS("mul1"),
		TRUNC(AllTypes.Decimal.MUL(Float(11.22)), Int(2)).AS("mul2"),
		TRUNC(AllTypes.Decimal.DIV(AllTypes.DecimalPtr), Int(2)).AS("div1"),
		TRUNC(AllTypes.Decimal.DIV(Float(11.22)), Int(2)).AS("div2"),
		TRUNC(AllTypes.Decimal.MOD(AllTypes.DecimalPtr), Int(2)).AS("mod1"),
		TRUNC(AllTypes.Decimal.MOD(Float(11.22)), Int(2)).AS("mod2"),
		TRUNC(AllTypes.Decimal.POW(AllTypes.DecimalPtr), Int(2)).AS("pow1"),
		TRUNC(AllTypes.Decimal.POW(Float(2.1)), Int(2)).AS("pow2"),

		TRUNC(ABSf(AllTypes.Decimal), Int(2)).AS("abs"),
		TRUNC(POWER(AllTypes.Decimal, Float(2.1)), Int(2)).AS("power"),
		TRUNC(SQRT(AllTypes.Decimal), Int(2)).AS("sqrt"),
		TRUNC(CAST(CBRT(AllTypes.Decimal)).AS_DECIMAL(), Int(2)).AS("cbrt"),

		CEIL(AllTypes.Real).AS("ceil"),
		FLOOR(AllTypes.Real).AS("floor"),
		ROUND(AllTypes.Decimal).AS("round1"),
		ROUND(AllTypes.Decimal, AllTypes.Integer).AS("round2"),

		SIGN(AllTypes.Real).AS("sign"),
		TRUNC(AllTypes.Decimal, Int(1)).AS("trunc"),
	).LIMIT(2)

	queryStr, _ := query.Sql()

	require.Equal(t, queryStr, `
SELECT (all_types.numeric = all_types.numeric) AS "eq1",
     (all_types.decimal = $1) AS "eq2",
     (all_types.real = $2) AS "eq3",
     (all_types.numeric IS DISTINCT FROM all_types.numeric) AS "distinct1",
     (all_types.decimal IS DISTINCT FROM $3) AS "distinct2",
     (all_types.real IS DISTINCT FROM $4) AS "distinct3",
     (all_types.numeric IS NOT DISTINCT FROM all_types.numeric) AS "not_distinct1",
     (all_types.decimal IS NOT DISTINCT FROM $5) AS "not_distinct2",
     (all_types.real IS NOT DISTINCT FROM $6) AS "not_distinct3",
     (all_types.numeric < $7) AS "lt1",
     (all_types.numeric < $8) AS "lt2",
     (all_types.numeric > $9) AS "gt1",
     (all_types.numeric > $10) AS "gt2",
     TRUNC((all_types.decimal + all_types.decimal), $11) AS "add1",
     TRUNC((all_types.decimal + $12), $13) AS "add2",
     TRUNC((all_types.decimal - all_types.decimal_ptr), $14) AS "sub1",
     TRUNC((all_types.decimal - $15), $16) AS "sub2",
     TRUNC((all_types.decimal * all_types.decimal_ptr), $17) AS "mul1",
     TRUNC((all_types.decimal * $18), $19) AS "mul2",
     TRUNC((all_types.decimal / all_types.decimal_ptr), $20) AS "div1",
     TRUNC((all_types.decimal / $21), $22) AS "div2",
     TRUNC((all_types.decimal % all_types.decimal_ptr), $23) AS "mod1",
     TRUNC((all_types.decimal % $24), $25) AS "mod2",
     TRUNC(POW(all_types.decimal, all_types.decimal_ptr), $26) AS "pow1",
     TRUNC(POW(all_types.decimal, $27), $28) AS "pow2",
     TRUNC(ABS(all_types.decimal), $29) AS "abs",
     TRUNC(POWER(all_types.decimal, $30), $31) AS "power",
     TRUNC(SQRT(all_types.decimal), $32) AS "sqrt",
     TRUNC(CBRT(all_types.decimal)::decimal, $33) AS "cbrt",
     CEIL(all_types.real) AS "ceil",
     FLOOR(all_types.real) AS "floor",
     ROUND(all_types.decimal) AS "round1",
     ROUND(all_types.decimal, all_types.integer) AS "round2",
     SIGN(all_types.real) AS "sign",
     TRUNC(all_types.decimal, $34) AS "trunc"
FROM test_sample.all_types
LIMIT $35;
`)

	var dest []struct {
		common.FloatExpressionTestResult `alias:"."`
	}

	err := query.Query(db, &dest)

	require.NoError(t, err)

	//testutils.PrintJson(dest)

	testutils.AssertJSONFile(t, dest, "./testdata/results/common/float_operators.json")
}

func TestIntegerOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.BigInt,
		AllTypes.BigIntPtr,
		AllTypes.SmallInt,
		AllTypes.SmallIntPtr,

		AllTypes.BigInt.EQ(AllTypes.BigInt).AS("eq1"),
		AllTypes.BigInt.EQ(Int(12)).AS("eq2"),

		AllTypes.BigInt.NOT_EQ(AllTypes.BigIntPtr).AS("neq1"),
		AllTypes.BigInt.NOT_EQ(Int(12)).AS("neq2"),

		AllTypes.BigInt.IS_DISTINCT_FROM(AllTypes.BigInt).AS("distinct1"),
		AllTypes.BigInt.IS_DISTINCT_FROM(Int(12)).AS("distinct2"),

		AllTypes.BigInt.IS_NOT_DISTINCT_FROM(AllTypes.BigInt).AS("not distinct1"),
		AllTypes.BigInt.IS_NOT_DISTINCT_FROM(Int(12)).AS("not distinct2"),

		AllTypes.BigInt.LT(AllTypes.BigIntPtr).AS("lt1"),
		AllTypes.BigInt.LT(Int(65)).AS("lt2"),

		AllTypes.BigInt.LT_EQ(AllTypes.BigIntPtr).AS("lte1"),
		AllTypes.BigInt.LT_EQ(Int(65)).AS("lte2"),

		AllTypes.BigInt.GT(AllTypes.BigIntPtr).AS("gt1"),
		AllTypes.BigInt.GT(Int(65)).AS("gt2"),

		AllTypes.BigInt.GT_EQ(AllTypes.BigIntPtr).AS("gte1"),
		AllTypes.BigInt.GT_EQ(Int(65)).AS("gte2"),

		AllTypes.BigInt.ADD(AllTypes.BigInt).AS("add1"),
		AllTypes.BigInt.ADD(Int(11)).AS("add2"),

		AllTypes.BigInt.SUB(AllTypes.BigInt).AS("sub1"),
		AllTypes.BigInt.SUB(Int(11)).AS("sub2"),

		AllTypes.BigInt.MUL(AllTypes.BigInt).AS("mul1"),
		AllTypes.BigInt.MUL(Int(11)).AS("mul2"),

		AllTypes.BigInt.DIV(AllTypes.BigInt).AS("div1"),
		AllTypes.BigInt.DIV(Int(11)).AS("div2"),

		AllTypes.BigInt.MOD(AllTypes.BigInt).AS("mod1"),
		AllTypes.BigInt.MOD(Int(11)).AS("mod2"),

		AllTypes.SmallInt.POW(AllTypes.SmallInt.DIV(Int(3))).AS("pow1"),
		AllTypes.SmallInt.POW(Int(6)).AS("pow2"),

		AllTypes.SmallInt.BIT_AND(AllTypes.SmallInt).AS("bit_and1"),
		AllTypes.SmallInt.BIT_AND(AllTypes.SmallInt).AS("bit_and2"),

		AllTypes.SmallInt.BIT_OR(AllTypes.SmallInt).AS("bit or 1"),
		AllTypes.SmallInt.BIT_OR(Int(22)).AS("bit or 2"),

		AllTypes.SmallInt.BIT_XOR(AllTypes.SmallInt).AS("bit xor 1"),
		AllTypes.SmallInt.BIT_XOR(Int(11)).AS("bit xor 2"),

		BIT_NOT(Int(-1).MUL(AllTypes.SmallInt)).AS("bit_not_1"),
		BIT_NOT(Int(-11)).AS("bit_not_2"),

		AllTypes.SmallInt.BIT_SHIFT_LEFT(AllTypes.SmallInt.DIV(Int(2))).AS("bit shift left 1"),
		AllTypes.SmallInt.BIT_SHIFT_LEFT(Int(4)).AS("bit shift left 2"),

		AllTypes.SmallInt.BIT_SHIFT_RIGHT(AllTypes.SmallInt.DIV(Int(5))).AS("bit shift right 1"),
		AllTypes.SmallInt.BIT_SHIFT_RIGHT(Int(1)).AS("bit shift right 2"),

		ABSi(AllTypes.BigInt).AS("abs"),
		SQRT(ABSi(AllTypes.BigInt)).AS("sqrt"),
		CBRT(ABSi(AllTypes.BigInt)).AS("cbrt"),
	).LIMIT(2)

	//fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, `
SELECT all_types.big_int AS "all_types.big_int",
     all_types.big_int_ptr AS "all_types.big_int_ptr",
     all_types.small_int AS "all_types.small_int",
     all_types.small_int_ptr AS "all_types.small_int_ptr",
     (all_types.big_int = all_types.big_int) AS "eq1",
     (all_types.big_int = $1) AS "eq2",
     (all_types.big_int != all_types.big_int_ptr) AS "neq1",
     (all_types.big_int != $2) AS "neq2",
     (all_types.big_int IS DISTINCT FROM all_types.big_int) AS "distinct1",
     (all_types.big_int IS DISTINCT FROM $3) AS "distinct2",
     (all_types.big_int IS NOT DISTINCT FROM all_types.big_int) AS "not distinct1",
     (all_types.big_int IS NOT DISTINCT FROM $4) AS "not distinct2",
     (all_types.big_int < all_types.big_int_ptr) AS "lt1",
     (all_types.big_int < $5) AS "lt2",
     (all_types.big_int <= all_types.big_int_ptr) AS "lte1",
     (all_types.big_int <= $6) AS "lte2",
     (all_types.big_int > all_types.big_int_ptr) AS "gt1",
     (all_types.big_int > $7) AS "gt2",
     (all_types.big_int >= all_types.big_int_ptr) AS "gte1",
     (all_types.big_int >= $8) AS "gte2",
     (all_types.big_int + all_types.big_int) AS "add1",
     (all_types.big_int + $9) AS "add2",
     (all_types.big_int - all_types.big_int) AS "sub1",
     (all_types.big_int - $10) AS "sub2",
     (all_types.big_int * all_types.big_int) AS "mul1",
     (all_types.big_int * $11) AS "mul2",
     (all_types.big_int / all_types.big_int) AS "div1",
     (all_types.big_int / $12) AS "div2",
     (all_types.big_int % all_types.big_int) AS "mod1",
     (all_types.big_int % $13) AS "mod2",
     POW(all_types.small_int, (all_types.small_int / $14)) AS "pow1",
     POW(all_types.small_int, $15) AS "pow2",
     (all_types.small_int & all_types.small_int) AS "bit_and1",
     (all_types.small_int & all_types.small_int) AS "bit_and2",
     (all_types.small_int | all_types.small_int) AS "bit or 1",
     (all_types.small_int | $16) AS "bit or 2",
     (all_types.small_int # all_types.small_int) AS "bit xor 1",
     (all_types.small_int # $17) AS "bit xor 2",
     (~ ($18 * all_types.small_int)) AS "bit_not_1",
     (~ -11) AS "bit_not_2",
     (all_types.small_int << (all_types.small_int / $19)) AS "bit shift left 1",
     (all_types.small_int << $20) AS "bit shift left 2",
     (all_types.small_int >> (all_types.small_int / $21)) AS "bit shift right 1",
     (all_types.small_int >> $22) AS "bit shift right 2",
     ABS(all_types.big_int) AS "abs",
     SQRT(ABS(all_types.big_int)) AS "sqrt",
     CBRT(ABS(all_types.big_int)) AS "cbrt"
FROM test_sample.all_types
LIMIT $23;
`)

	var dest []struct {
		common.AllTypesIntegerExpResult `alias:"."`
	}

	err := query.Query(db, &dest)

	require.NoError(t, err)

	//testutils.SaveJsonFile("./testdata/common/int_operators.json", dest)
	//testutils.PrintJson(dest)
	testutils.AssertJSONFile(t, dest, "./testdata/results/common/int_operators.json")
}

func TestTimeExpression(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Time.EQ(AllTypes.Time),
		AllTypes.Time.EQ(Time(23, 6, 6, 1)),
		AllTypes.Timez.EQ(AllTypes.TimezPtr),
		AllTypes.Timez.EQ(Timez(23, 6, 6, 222, "+200")),
		AllTypes.Timestamp.EQ(AllTypes.TimestampPtr),
		AllTypes.Timestamp.EQ(Timestamp(2010, 10, 21, 15, 30, 12, 333)),
		AllTypes.Timestampz.EQ(AllTypes.TimestampzPtr),
		AllTypes.Timestampz.EQ(Timestampz(2010, 10, 21, 15, 30, 12, 444, "PST")),
		AllTypes.Date.EQ(AllTypes.DatePtr),
		AllTypes.Date.EQ(Date(2010, 12, 3)),

		AllTypes.Time.NOT_EQ(AllTypes.Time),
		AllTypes.Time.NOT_EQ(Time(23, 6, 6, 10)),
		AllTypes.Timez.NOT_EQ(AllTypes.TimezPtr),
		AllTypes.Timez.NOT_EQ(Timez(23, 6, 6, 555, "+4:00")),
		AllTypes.Timestamp.NOT_EQ(AllTypes.TimestampPtr),
		AllTypes.Timestamp.NOT_EQ(Timestamp(2010, 10, 21, 15, 30, 12, 666)),
		AllTypes.Timestampz.NOT_EQ(AllTypes.TimestampzPtr),
		AllTypes.Timestampz.NOT_EQ(Timestampz(2010, 10, 21, 15, 30, 12, 777, "UTC")),
		AllTypes.Date.NOT_EQ(AllTypes.DatePtr),
		AllTypes.Date.NOT_EQ(Date(2010, 12, 3)),

		AllTypes.Time.IS_DISTINCT_FROM(AllTypes.Time),
		AllTypes.Time.IS_DISTINCT_FROM(Time(23, 6, 6, 100)),

		AllTypes.Time.IS_NOT_DISTINCT_FROM(AllTypes.Time),
		AllTypes.Time.IS_NOT_DISTINCT_FROM(Time(23, 6, 6, 200)),

		AllTypes.Time.LT(AllTypes.Time),
		AllTypes.Time.LT(Time(23, 6, 6, 22)),

		AllTypes.Time.LT_EQ(AllTypes.Time),
		AllTypes.Time.LT_EQ(Time(23, 6, 6, 33)),

		AllTypes.Time.GT(AllTypes.Time),
		AllTypes.Time.GT(Time(23, 6, 6, 0)),

		AllTypes.Time.GT_EQ(AllTypes.Time),
		AllTypes.Time.GT_EQ(Time(23, 6, 6, 1)),

		AllTypes.Date.ADD(INTERVAL(1, HOUR)),
		AllTypes.Date.SUB(INTERVAL(1, MINUTE)),
		AllTypes.Time.ADD(INTERVAL(1, HOUR)),
		AllTypes.Time.SUB(INTERVAL(1, MINUTE)),
		AllTypes.Timez.ADD(INTERVAL(1, HOUR)),
		AllTypes.Timez.SUB(INTERVAL(1, MINUTE)),
		AllTypes.Timestamp.ADD(INTERVAL(1, HOUR)),
		AllTypes.Timestamp.SUB(INTERVAL(1, MINUTE)),
		AllTypes.Timestampz.ADD(INTERVAL(1, HOUR)),
		AllTypes.Timestampz.SUB(INTERVAL(1, MINUTE)),

		AllTypes.Date.SUB(CAST(String("04:05:06")).AS_INTERVAL()),

		CURRENT_DATE(),
		CURRENT_TIME(),
		CURRENT_TIME(2),
		CURRENT_TIMESTAMP(),
		CURRENT_TIMESTAMP(1),
		LOCALTIME(),
		LOCALTIME(11),
		LOCALTIMESTAMP(),
		LOCALTIMESTAMP(4),
		NOW(),
	)

	//fmt.Println(query.DebugSql())

	dest := []struct{}{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestInterval(t *testing.T) {
	stmt := SELECT(
		INTERVAL(1, YEAR),
		INTERVAL(1, MONTH),
		INTERVAL(1, WEEK),
		INTERVAL(1, DAY),
		INTERVAL(1, HOUR),
		INTERVAL(1, MINUTE),
		INTERVAL(1, SECOND),
		INTERVAL(1, MILLISECOND),
		INTERVAL(1, MICROSECOND),
		INTERVAL(1, DECADE),
		INTERVAL(1, CENTURY),
		INTERVAL(1, MILLENNIUM),

		INTERVAL(1, YEAR, 10, MONTH),
		INTERVAL(1, YEAR, 10, MONTH, 20, DAY),
		INTERVAL(1, YEAR, 10, MONTH, 20, DAY, 3, HOUR),

		INTERVAL(1, YEAR).IS_NOT_NULL(),
		INTERVAL(1, YEAR).AS("one year"),

		INTERVALd(0),
		INTERVALd(1*time.Microsecond),
		INTERVALd(1*time.Millisecond),
		INTERVALd(1*time.Second),
		INTERVALd(1*time.Minute),
		INTERVALd(1*time.Hour),
		INTERVALd(24*time.Hour),
		INTERVALd(24*time.Hour+2*time.Hour+3*time.Minute+4*time.Second+5*time.Microsecond),

		AllTypes.Interval.EQ(INTERVAL(2, HOUR, 20, MINUTE)).EQ(Bool(true)),
		AllTypes.IntervalPtr.NOT_EQ(INTERVAL(2, HOUR, 20, MINUTE)).EQ(Bool(false)),
		AllTypes.Interval.IS_DISTINCT_FROM(INTERVAL(2, HOUR, 20, MINUTE)).EQ(AllTypes.Boolean),
		AllTypes.IntervalPtr.IS_NOT_DISTINCT_FROM(INTERVALd(10*time.Microsecond)).EQ(AllTypes.Boolean),
		AllTypes.Interval.LT(AllTypes.IntervalPtr).EQ(AllTypes.BooleanPtr),
		AllTypes.Interval.LT_EQ(AllTypes.IntervalPtr).EQ(AllTypes.BooleanPtr),
		AllTypes.Interval.GT(AllTypes.IntervalPtr).EQ(AllTypes.BooleanPtr),
		AllTypes.Interval.GT_EQ(AllTypes.IntervalPtr).EQ(AllTypes.BooleanPtr),
		AllTypes.Interval.ADD(AllTypes.IntervalPtr).EQ(INTERVALd(17*time.Second)),
		AllTypes.Interval.SUB(AllTypes.IntervalPtr).EQ(INTERVAL(100, MICROSECOND)),
		AllTypes.IntervalPtr.MUL(Int(11)).EQ(AllTypes.Interval),
		AllTypes.IntervalPtr.DIV(Float(22.222)).EQ(AllTypes.IntervalPtr),
	).FROM(AllTypes)

	//fmt.Println(stmt.DebugSql())

	err := stmt.Query(db, &struct{}{})
	require.NoError(t, err)
	requireLogged(t, stmt)
}

func TestSubQueryColumnReference(t *testing.T) {
	skipForPgxDriver(t) // pgx driver returns time with time zone as string value

	type expected struct {
		sql  string
		args []interface{}
	}

	subQueries := map[SelectTable]expected{}

	selectSubQuery := AllTypes.SELECT(
		AllTypes.Boolean,
		AllTypes.Integer,
		AllTypes.Real,
		AllTypes.Text,
		AllTypes.Time,
		AllTypes.Timez,
		AllTypes.Timestamp,
		AllTypes.Timestampz,
		AllTypes.Date,
		AllTypes.Bytea.AS("aliasedColumn"),
	).
		LIMIT(2).
		AsTable("subQuery")

	var selectexpectedSQL = ` (
          SELECT all_types.boolean AS "all_types.boolean",
               all_types.integer AS "all_types.integer",
               all_types.real AS "all_types.real",
               all_types.text AS "all_types.text",
               all_types.time AS "all_types.time",
               all_types.timez AS "all_types.timez",
               all_types.timestamp AS "all_types.timestamp",
               all_types.timestampz AS "all_types.timestampz",
               all_types.date AS "all_types.date",
               all_types.bytea AS "aliasedColumn"
          FROM test_sample.all_types
          LIMIT 2
     ) AS "subQuery"`

	unionSubQuery :=
		UNION_ALL(
			AllTypes.SELECT(
				AllTypes.Boolean,
				AllTypes.Integer,
				AllTypes.Real,
				AllTypes.Text,
				AllTypes.Time,
				AllTypes.Timez,
				AllTypes.Timestamp,
				AllTypes.Timestampz,
				AllTypes.Date,
				AllTypes.Bytea.AS("aliasedColumn"),
			).
				LIMIT(1),
			AllTypes.SELECT(
				AllTypes.Boolean,
				AllTypes.Integer,
				AllTypes.Real,
				AllTypes.Text,
				AllTypes.Time,
				AllTypes.Timez,
				AllTypes.Timestamp,
				AllTypes.Timestampz,
				AllTypes.Date,
				AllTypes.Bytea.AS("aliasedColumn"),
			).
				LIMIT(1).OFFSET(1),
		).
			AsTable("subQuery")

	unionexpectedSQL := ` (
          (
               SELECT all_types.boolean AS "all_types.boolean",
                    all_types.integer AS "all_types.integer",
                    all_types.real AS "all_types.real",
                    all_types.text AS "all_types.text",
                    all_types.time AS "all_types.time",
                    all_types.timez AS "all_types.timez",
                    all_types.timestamp AS "all_types.timestamp",
                    all_types.timestampz AS "all_types.timestampz",
                    all_types.date AS "all_types.date",
                    all_types.bytea AS "aliasedColumn"
               FROM test_sample.all_types
               LIMIT 1
          )
          UNION ALL
          (
               SELECT all_types.boolean AS "all_types.boolean",
                    all_types.integer AS "all_types.integer",
                    all_types.real AS "all_types.real",
                    all_types.text AS "all_types.text",
                    all_types.time AS "all_types.time",
                    all_types.timez AS "all_types.timez",
                    all_types.timestamp AS "all_types.timestamp",
                    all_types.timestampz AS "all_types.timestampz",
                    all_types.date AS "all_types.date",
                    all_types.bytea AS "aliasedColumn"
               FROM test_sample.all_types
               LIMIT 1
               OFFSET 1
          )
     ) AS "subQuery"`

	subQueries[unionSubQuery] = expected{sql: unionexpectedSQL, args: []interface{}{int64(1), int64(1), int64(1)}}
	subQueries[selectSubQuery] = expected{sql: selectexpectedSQL, args: []interface{}{int64(2)}}

	for subQuery, expected := range subQueries {
		boolColumn := AllTypes.Boolean.From(subQuery)
		intColumn := AllTypes.Integer.From(subQuery)
		floatColumn := AllTypes.Real.From(subQuery)
		stringColumn := AllTypes.Text.From(subQuery)
		timeColumn := AllTypes.Time.From(subQuery)
		timezColumn := AllTypes.Timez.From(subQuery)
		timestampColumn := AllTypes.Timestamp.From(subQuery)
		timestampzColumn := AllTypes.Timestampz.From(subQuery)
		dateColumn := AllTypes.Date.From(subQuery)
		aliasedColumn := StringColumn("aliasedColumn").From(subQuery)

		stmt1 := SELECT(
			boolColumn,
			intColumn,
			floatColumn,
			stringColumn,
			timeColumn,
			timezColumn,
			timestampColumn,
			timestampzColumn,
			dateColumn,
			aliasedColumn,
		).
			FROM(subQuery)

		var expectedSQL = `
SELECT "subQuery"."all_types.boolean" AS "all_types.boolean",
     "subQuery"."all_types.integer" AS "all_types.integer",
     "subQuery"."all_types.real" AS "all_types.real",
     "subQuery"."all_types.text" AS "all_types.text",
     "subQuery"."all_types.time" AS "all_types.time",
     "subQuery"."all_types.timez" AS "all_types.timez",
     "subQuery"."all_types.timestamp" AS "all_types.timestamp",
     "subQuery"."all_types.timestampz" AS "all_types.timestampz",
     "subQuery"."all_types.date" AS "all_types.date",
     "subQuery"."aliasedColumn" AS "aliasedColumn"
FROM`

		testutils.AssertDebugStatementSql(t, stmt1, expectedSQL+expected.sql+";\n", expected.args...)

		dest1 := []model.AllTypes{}
		err := stmt1.Query(db, &dest1)
		require.NoError(t, err)
		require.Equal(t, len(dest1), 2)
		require.Equal(t, dest1[0].Boolean, allTypesRow0.Boolean)
		require.Equal(t, dest1[0].Integer, allTypesRow0.Integer)
		require.Equal(t, dest1[0].Real, allTypesRow0.Real)
		require.Equal(t, dest1[0].Text, allTypesRow0.Text)
		testutils.AssertDeepEqual(t, dest1[0].Time, allTypesRow0.Time)
		testutils.AssertDeepEqual(t, dest1[0].Timez, allTypesRow0.Timez)
		testutils.AssertDeepEqual(t, dest1[0].Timestamp, allTypesRow0.Timestamp)
		testutils.AssertDeepEqual(t, dest1[0].Timestampz, allTypesRow0.Timestampz)
		testutils.AssertDeepEqual(t, dest1[0].Date, allTypesRow0.Date)

		stmt2 := SELECT(
			subQuery.AllColumns(),
		).
			FROM(subQuery)

		testutils.AssertDebugStatementSql(t, stmt2, expectedSQL+expected.sql+";\n", expected.args...)

		dest2 := []model.AllTypes{}
		err = stmt2.Query(db, &dest2)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest1, dest2)
		requireLogged(t, stmt2)
	}
}

func TestTimeLiterals(t *testing.T) {
	skipForPgxDriver(t) // pgx driver returns time with time zone as string

	loc, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	var timeT = time.Date(2009, 11, 17, 20, 34, 58, 651387237, loc)

	query := SELECT(
		DateT(timeT).AS("date"),
		TimeT(timeT).AS("time"),
		TimezT(timeT).AS("timez"),
		TimestampT(timeT).AS("timestamp"),
		TimestampzT(timeT).AS("timestampz"),
	).FROM(AllTypes).
		LIMIT(1)

	//fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, `
SELECT $1::date AS "date",
     $2::time without time zone AS "time",
     $3::time with time zone AS "timez",
     $4::timestamp without time zone AS "timestamp",
     $5::timestamp with time zone AS "timestampz"
FROM test_sample.all_types
LIMIT $6;
`)

	var dest struct {
		Date      time.Time
		Time      time.Time
		Timez     time.Time
		Timestamp time.Time
		//Timestampz time.Time
	}

	err = query.Query(db, &dest)

	require.NoError(t, err)

	//testutils.PrintJson(dest)

	testutils.AssertJSON(t, dest, `
{
	"Date": "2009-11-17T00:00:00Z",
	"Time": "0000-01-01T20:34:58.651387Z",
	"Timez": "0000-01-01T20:34:58.651387+01:00",
	"Timestamp": "2009-11-17T20:34:58.651387Z"
}
`)
	requireLogged(t, query)
}

var allTypesRow0 = model.AllTypes{
	SmallIntPtr:        testutils.Int16Ptr(14),
	SmallInt:           14,
	IntegerPtr:         testutils.Int32Ptr(300),
	Integer:            300,
	BigIntPtr:          testutils.Int64Ptr(50000),
	BigInt:             5000,
	DecimalPtr:         testutils.Float64Ptr(1.11),
	Decimal:            1.11,
	NumericPtr:         testutils.Float64Ptr(2.22),
	Numeric:            2.22,
	RealPtr:            testutils.Float32Ptr(5.55),
	Real:               5.55,
	DoublePrecisionPtr: testutils.Float64Ptr(11111111.22),
	DoublePrecision:    11111111.22,
	Smallserial:        1,
	Serial:             1,
	Bigserial:          1,
	//MoneyPtr: nil,
	//Money:
	VarCharPtr:           testutils.StringPtr("ABBA"),
	VarChar:              "ABBA",
	CharPtr:              testutils.StringPtr("JOHN                                                                            "),
	Char:                 "JOHN                                                                            ",
	TextPtr:              testutils.StringPtr("Some text"),
	Text:                 "Some text",
	ByteaPtr:             testutils.ByteArrayPtr([]byte("bytea")),
	Bytea:                []byte("bytea"),
	TimestampzPtr:        testutils.TimestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	Timestampz:           *testutils.TimestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	TimestampPtr:         testutils.TimestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	Timestamp:            *testutils.TimestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	DatePtr:              testutils.TimestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	Date:                 *testutils.TimestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	TimezPtr:             testutils.TimeWithTimeZone("04:05:06 -0800"),
	Timez:                *testutils.TimeWithTimeZone("04:05:06 -0800"),
	TimePtr:              testutils.TimeWithoutTimeZone("04:05:06"),
	Time:                 *testutils.TimeWithoutTimeZone("04:05:06"),
	IntervalPtr:          testutils.StringPtr("3 days 04:05:06"),
	Interval:             "3 days 04:05:06",
	BooleanPtr:           testutils.BoolPtr(true),
	Boolean:              false,
	PointPtr:             testutils.StringPtr("(2,3)"),
	BitPtr:               testutils.StringPtr("101"),
	Bit:                  "101",
	BitVaryingPtr:        testutils.StringPtr("101111"),
	BitVarying:           "101111",
	TsvectorPtr:          testutils.StringPtr("'supernova':1"),
	Tsvector:             "'supernova':1",
	UUIDPtr:              testutils.UUIDPtr("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	UUID:                 uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	XMLPtr:               testutils.StringPtr("<Sub>abc</Sub>"),
	XML:                  "<Sub>abc</Sub>",
	JSONPtr:              testutils.StringPtr(`{"a": 1, "b": 3}`),
	JSON:                 `{"a": 1, "b": 3}`,
	JsonbPtr:             testutils.StringPtr(`{"a": 1, "b": 3}`),
	Jsonb:                `{"a": 1, "b": 3}`,
	IntegerArrayPtr:      testutils.StringPtr("{1,2,3}"),
	IntegerArray:         "{1,2,3}",
	TextArrayPtr:         testutils.StringPtr("{breakfast,consulting}"),
	TextArray:            "{breakfast,consulting}",
	JsonbArray:           `{"{\"a\": 1, \"b\": 2}","{\"a\": 3, \"b\": 4}"}`,
	TextMultiDimArrayPtr: testutils.StringPtr("{{meeting,lunch},{training,presentation}}"),
	TextMultiDimArray:    "{{meeting,lunch},{training,presentation}}",
}

var allTypesRow1 = model.AllTypes{
	SmallIntPtr:        nil,
	SmallInt:           14,
	IntegerPtr:         nil,
	Integer:            300,
	BigIntPtr:          nil,
	BigInt:             5000,
	DecimalPtr:         nil,
	Decimal:            1.11,
	NumericPtr:         nil,
	Numeric:            2.22,
	RealPtr:            nil,
	Real:               5.55,
	DoublePrecisionPtr: nil,
	DoublePrecision:    11111111.22,
	Smallserial:        2,
	Serial:             2,
	Bigserial:          2,
	//MoneyPtr: nil,
	//Money:
	VarCharPtr:           nil,
	VarChar:              "ABBA",
	CharPtr:              nil,
	Char:                 "JOHN                                                                            ",
	TextPtr:              nil,
	Text:                 "Some text",
	ByteaPtr:             nil,
	Bytea:                []byte("bytea"),
	TimestampzPtr:        nil,
	Timestampz:           *testutils.TimestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	TimestampPtr:         nil,
	Timestamp:            *testutils.TimestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	DatePtr:              nil,
	Date:                 *testutils.TimestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	TimezPtr:             nil,
	Timez:                *testutils.TimeWithTimeZone("04:05:06 -0800"),
	TimePtr:              nil,
	Time:                 *testutils.TimeWithoutTimeZone("04:05:06"),
	IntervalPtr:          nil,
	Interval:             "3 days 04:05:06",
	BooleanPtr:           nil,
	Boolean:              false,
	PointPtr:             nil,
	BitPtr:               nil,
	Bit:                  "101",
	BitVaryingPtr:        nil,
	BitVarying:           "101111",
	TsvectorPtr:          nil,
	Tsvector:             "'supernova':1",
	UUIDPtr:              nil,
	UUID:                 uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	XMLPtr:               nil,
	XML:                  "<Sub>abc</Sub>",
	JSONPtr:              nil,
	JSON:                 `{"a": 1, "b": 3}`,
	JsonbPtr:             nil,
	Jsonb:                `{"a": 1, "b": 3}`,
	IntegerArrayPtr:      nil,
	IntegerArray:         "{1,2,3}",
	TextArrayPtr:         nil,
	TextArray:            "{breakfast,consulting}",
	JsonbArray:           `{"{\"a\": 1, \"b\": 2}","{\"a\": 3, \"b\": 4}"}`,
	TextMultiDimArrayPtr: nil,
	TextMultiDimArray:    "{{meeting,lunch},{training,presentation}}",
}
