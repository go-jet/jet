package mysql

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-jet/jet/internal/testutils"
	"github.com/go-jet/jet/tests/.gentestdata/mysql/test_sample/model"
	. "github.com/go-jet/jet/tests/.gentestdata/mysql/test_sample/table"
	"github.com/go-jet/jet/tests/.gentestdata/mysql/test_sample/view"
	"github.com/go-jet/jet/tests/testdata/results/common"

	. "github.com/go-jet/jet/mysql"
)

func TestAllTypes(t *testing.T) {

	dest := []model.AllTypes{}

	err := AllTypes.
		SELECT(AllTypes.AllColumns).
		LIMIT(2).
		Query(db, &dest)

	require.NoError(t, err)

	require.Equal(t, len(dest), 2)

	if sourceIsMariaDB() { // MariaDB saves current timestamp in a case of NULL value insert
		return
	}

	//testutils.PrintJson(dest)
	testutils.AssertJSON(t, dest, allTypesJson)
}

func TestAllTypesViewSelect(t *testing.T) {

	type AllTypesView model.AllTypes

	dest := []AllTypesView{}

	err := view.AllTypesView.SELECT(view.AllTypesView.AllColumns).Query(db, &dest)
	require.NoError(t, err)
	require.Equal(t, len(dest), 2)

	if sourceIsMariaDB() { // MariaDB saves current timestamp in a case of NULL value insert
		return
	}

	testutils.AssertJSON(t, dest, allTypesJson)
}

func TestUUID(t *testing.T) {

	query := AllTypes.
		SELECT(
			Raw("uuid()").AS("uuid"),
			String("dc8daae3-b83b-11e9-8eb4-98ded00c39c6").AS("str_uuid"),
			Raw("unhex(replace('dc8daae3-b83b-11e9-8eb4-98ded00c39c6','-',''))").AS("bin_uuid"),
		).LIMIT(1)

	//fmt.Println(query.DebugSql())

	var dest struct {
		UUID    uuid.UUID
		StrUUID *uuid.UUID
		BinUUID uuid.UUID
	}

	err := query.Query(db, &dest)

	require.NoError(t, err)
	require.True(t, dest.StrUUID != nil)
	require.True(t, dest.UUID.String() != uuid.UUID{}.String())
	require.True(t, dest.StrUUID.String() != uuid.UUID{}.String())
	require.Equal(t, dest.StrUUID.String(), dest.BinUUID.String())
}

func TestExpressionOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Integer.IS_NULL().AS("result.is_null"),
		AllTypes.DatePtr.IS_NOT_NULL().AS("result.is_not_null"),
		AllTypes.SmallIntPtr.IN(Int(11), Int(22)).AS("result.in"),
		AllTypes.SmallIntPtr.IN(AllTypes.SELECT(AllTypes.Integer)).AS("result.in_select"),
		AllTypes.SmallIntPtr.NOT_IN(Int(11), Int(22), NULL).AS("result.not_in"),
		AllTypes.SmallIntPtr.NOT_IN(AllTypes.SELECT(AllTypes.Integer)).AS("result.not_in_select"),

		Raw("DATABASE()"),
	).LIMIT(2)

	//fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, strings.Replace(`
SELECT all_types.'integer' IS NULL AS "result.is_null",
     all_types.date_ptr IS NOT NULL AS "result.is_not_null",
     (all_types.small_int_ptr IN (?, ?)) AS "result.in",
     (all_types.small_int_ptr IN ((
          SELECT all_types.'integer' AS "all_types.integer"
          FROM test_sample.all_types
     ))) AS "result.in_select",
     (all_types.small_int_ptr NOT IN (?, ?, NULL)) AS "result.not_in",
     (all_types.small_int_ptr NOT IN ((
          SELECT all_types.'integer' AS "all_types.integer"
          FROM test_sample.all_types
     ))) AS "result.not_in_select",
     DATABASE()
FROM test_sample.all_types
LIMIT ?;
`, "'", "`", -1), int64(11), int64(22), int64(11), int64(22), int64(2))

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
		"NotIn": null,
		"NotInSelect": true
	},
	{
		"IsNull": false,
		"IsNotNull": false,
		"In": null,
		"InSelect": null,
		"NotIn": null,
		"NotInSelect": null
	}
]
`)
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
	)

	//fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, `
SELECT (all_types.boolean = all_types.boolean_ptr) AS "EQ1",
     (all_types.boolean = ?) AS "EQ2",
     (all_types.boolean != all_types.boolean_ptr) AS "NEq1",
     (all_types.boolean != ?) AS "NEq2",
     (NOT(all_types.boolean <=> all_types.boolean_ptr)) AS "distinct1",
     (NOT(all_types.boolean <=> ?)) AS "distinct2",
     (all_types.boolean <=> all_types.boolean_ptr) AS "not_distinct_1",
     (all_types.boolean <=> ?) AS "NOTDISTINCT2",
     all_types.boolean IS TRUE AS "ISTRUE",
     all_types.boolean IS NOT TRUE AS "isnottrue",
     all_types.boolean IS FALSE AS "is_False",
     all_types.boolean IS NOT FALSE AS "is not false",
     all_types.boolean IS UNKNOWN AS "is unknown",
     all_types.boolean IS NOT UNKNOWN AS "is_not_unknown",
     ((all_types.boolean AND all_types.boolean) = (all_types.boolean AND all_types.boolean)) AS "complex1",
     ((all_types.boolean OR all_types.boolean) = (all_types.boolean AND all_types.boolean)) AS "complex2"
FROM test_sample.all_types;
`, true, false, true, true)

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
		TRUNC(CBRT(AllTypes.Decimal), Int(2)).AS("cbrt"),

		CEIL(AllTypes.Real).AS("ceil"),
		FLOOR(AllTypes.Real).AS("floor"),
		ROUND(AllTypes.Decimal).AS("round1"),
		ROUND(AllTypes.Decimal, Int(2)).AS("round2"),

		SIGN(AllTypes.Real).AS("sign"),
		TRUNC(AllTypes.Decimal, Int(1)).AS("trunc"),
	).LIMIT(2)

	queryStr, _ := query.Sql()

	//fmt.Println(queryStr)

	require.Equal(t, queryStr, strings.Replace(`
SELECT (all_types.'numeric' = all_types.'numeric') AS "eq1",
     (all_types.'decimal' = ?) AS "eq2",
     (all_types.'real' = ?) AS "eq3",
     (NOT(all_types.'numeric' <=> all_types.'numeric')) AS "distinct1",
     (NOT(all_types.'decimal' <=> ?)) AS "distinct2",
     (NOT(all_types.'real' <=> ?)) AS "distinct3",
     (all_types.'numeric' <=> all_types.'numeric') AS "not_distinct1",
     (all_types.'decimal' <=> ?) AS "not_distinct2",
     (all_types.'real' <=> ?) AS "not_distinct3",
     (all_types.'numeric' < ?) AS "lt1",
     (all_types.'numeric' < ?) AS "lt2",
     (all_types.'numeric' > ?) AS "gt1",
     (all_types.'numeric' > ?) AS "gt2",
     TRUNCATE((all_types.'decimal' + all_types.'decimal'), ?) AS "add1",
     TRUNCATE((all_types.'decimal' + ?), ?) AS "add2",
     TRUNCATE((all_types.'decimal' - all_types.decimal_ptr), ?) AS "sub1",
     TRUNCATE((all_types.'decimal' - ?), ?) AS "sub2",
     TRUNCATE((all_types.'decimal' * all_types.decimal_ptr), ?) AS "mul1",
     TRUNCATE((all_types.'decimal' * ?), ?) AS "mul2",
     TRUNCATE((all_types.'decimal' / all_types.decimal_ptr), ?) AS "div1",
     TRUNCATE((all_types.'decimal' / ?), ?) AS "div2",
     TRUNCATE((all_types.'decimal' % all_types.decimal_ptr), ?) AS "mod1",
     TRUNCATE((all_types.'decimal' % ?), ?) AS "mod2",
     TRUNCATE(POW(all_types.'decimal', all_types.decimal_ptr), ?) AS "pow1",
     TRUNCATE(POW(all_types.'decimal', ?), ?) AS "pow2",
     TRUNCATE(ABS(all_types.'decimal'), ?) AS "abs",
     TRUNCATE(POWER(all_types.'decimal', ?), ?) AS "power",
     TRUNCATE(SQRT(all_types.'decimal'), ?) AS "sqrt",
     TRUNCATE(POWER(all_types.'decimal', (? / ?)), ?) AS "cbrt",
     CEIL(all_types.'real') AS "ceil",
     FLOOR(all_types.'real') AS "floor",
     ROUND(all_types.'decimal') AS "round1",
     ROUND(all_types.'decimal', ?) AS "round2",
     SIGN(all_types.'real') AS "sign",
     TRUNCATE(all_types.'decimal', ?) AS "trunc"
FROM test_sample.all_types
LIMIT ?;
`, "'", "`", -1))

	var dest []struct {
		common.FloatExpressionTestResult `alias:"."`
	}

	err := query.Query(db, &dest)

	require.NoError(t, err)

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
		BIT_NOT(Int(-1).MUL(Int(11))).AS("bit_not_2"),

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
     (all_types.big_int = ?) AS "eq2",
     (all_types.big_int != all_types.big_int_ptr) AS "neq1",
     (all_types.big_int != ?) AS "neq2",
     (NOT(all_types.big_int <=> all_types.big_int)) AS "distinct1",
     (NOT(all_types.big_int <=> ?)) AS "distinct2",
     (all_types.big_int <=> all_types.big_int) AS "not distinct1",
     (all_types.big_int <=> ?) AS "not distinct2",
     (all_types.big_int < all_types.big_int_ptr) AS "lt1",
     (all_types.big_int < ?) AS "lt2",
     (all_types.big_int <= all_types.big_int_ptr) AS "lte1",
     (all_types.big_int <= ?) AS "lte2",
     (all_types.big_int > all_types.big_int_ptr) AS "gt1",
     (all_types.big_int > ?) AS "gt2",
     (all_types.big_int >= all_types.big_int_ptr) AS "gte1",
     (all_types.big_int >= ?) AS "gte2",
     (all_types.big_int + all_types.big_int) AS "add1",
     (all_types.big_int + ?) AS "add2",
     (all_types.big_int - all_types.big_int) AS "sub1",
     (all_types.big_int - ?) AS "sub2",
     (all_types.big_int * all_types.big_int) AS "mul1",
     (all_types.big_int * ?) AS "mul2",
     (all_types.big_int DIV all_types.big_int) AS "div1",
     (all_types.big_int DIV ?) AS "div2",
     (all_types.big_int % all_types.big_int) AS "mod1",
     (all_types.big_int % ?) AS "mod2",
     POW(all_types.small_int, (all_types.small_int DIV ?)) AS "pow1",
     POW(all_types.small_int, ?) AS "pow2",
     (all_types.small_int & all_types.small_int) AS "bit_and1",
     (all_types.small_int & all_types.small_int) AS "bit_and2",
     (all_types.small_int | all_types.small_int) AS "bit or 1",
     (all_types.small_int | ?) AS "bit or 2",
     (all_types.small_int ^ all_types.small_int) AS "bit xor 1",
     (all_types.small_int ^ ?) AS "bit xor 2",
     (~ (? * all_types.small_int)) AS "bit_not_1",
     (~ (? * ?)) AS "bit_not_2",
     (all_types.small_int << (all_types.small_int DIV ?)) AS "bit shift left 1",
     (all_types.small_int << ?) AS "bit shift left 2",
     (all_types.small_int >> (all_types.small_int DIV ?)) AS "bit shift right 1",
     (all_types.small_int >> ?) AS "bit shift right 2",
     ABS(all_types.big_int) AS "abs",
     SQRT(ABS(all_types.big_int)) AS "sqrt",
     POWER(ABS(all_types.big_int), (? / ?)) AS "cbrt"
FROM test_sample.all_types
LIMIT ?;
`)

	var dest []struct {
		common.AllTypesIntegerExpResult `alias:"."`
	}

	err := query.Query(db, &dest)

	require.NoError(t, err)

	//testutils.PrintJson(dest)

	testutils.AssertJSONFile(t, dest, "./testdata/results/common/int_operators.json")
}

func TestStringOperators(t *testing.T) {

	var projectionList = []Projection{
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
		AllTypes.Text.LT_EQ(AllTypes.VarCharPtr),
		AllTypes.Text.LT_EQ(String("Text")),
		AllTypes.Text.CONCAT(String("text2")),
		AllTypes.Text.CONCAT(Int(11)),
		AllTypes.Text.LIKE(String("abc")),
		AllTypes.Text.NOT_LIKE(String("_b_")),
		AllTypes.Text.REGEXP_LIKE(String("aba")),
		AllTypes.Text.REGEXP_LIKE(String("aba"), false),
		String("ABA").REGEXP_LIKE(String("aba"), true),
		AllTypes.Text.NOT_REGEXP_LIKE(String("aba")),
		AllTypes.Text.NOT_REGEXP_LIKE(String("aba"), false),
		String("ABA").NOT_REGEXP_LIKE(String("aba"), true),

		BIT_LENGTH(AllTypes.Text),
		CHAR_LENGTH(AllTypes.Char),
		OCTET_LENGTH(AllTypes.Text),
		LOWER(AllTypes.VarCharPtr),
		UPPER(AllTypes.Char),
		LTRIM(AllTypes.VarCharPtr),
		RTRIM(AllTypes.VarCharPtr),
		CONCAT(String("string1"), Int(1), Float(11.12)),
		CONCAT_WS(String("string1"), Int(1), Float(11.12)),
		FORMAT(String("Hello %s, %1$s"), String("World")),
		LEFT(String("abcde"), Int(2)),
		RIGHT(String("abcde"), Int(2)),
		LENGTH(String("jose")),
		LPAD(String("Hi"), Int(5), String("xy")),
		RPAD(String("Hi"), Int(5), String("xy")),
		MD5(AllTypes.VarCharPtr),
		REPEAT(AllTypes.Text, Int(33)),
		REPLACE(AllTypes.Char, String("BA"), String("AB")),
		REVERSE(AllTypes.VarCharPtr),
		SUBSTR(AllTypes.CharPtr, Int(3)),
		SUBSTR(AllTypes.CharPtr, Int(3), Int(2)),
	}

	if !sourceIsMariaDB() {
		projectionList = append(projectionList, []Projection{
			REGEXP_LIKE(String("ABA"), String("aba")),
			REGEXP_LIKE(String("ABA"), String("aba"), "i"),
			REGEXP_LIKE(AllTypes.Text, String("aba"), "i"),
		}...)
	}

	query := SELECT(projectionList[0], projectionList[1:]...).
		FROM(AllTypes)

	//fmt.Println(query.DebugSql())

	dest := []struct{}{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

var timeT = time.Date(2009, 11, 17, 20, 34, 58, 651387237, time.UTC)

func TestTimeExpressions(t *testing.T) {

	query := AllTypes.SELECT(
		Time(timeT.Clock()),

		AllTypes.Time.EQ(AllTypes.Time),
		AllTypes.Time.EQ(Time(23, 6, 6)),
		AllTypes.Time.EQ(Time(22, 6, 6, 11*time.Millisecond)),
		AllTypes.Time.EQ(Time(21, 6, 6, 11111*time.Microsecond)),

		AllTypes.TimePtr.NOT_EQ(AllTypes.Time),
		AllTypes.TimePtr.NOT_EQ(Time(20, 16, 6)),

		AllTypes.Time.IS_DISTINCT_FROM(AllTypes.Time),
		AllTypes.Time.IS_DISTINCT_FROM(Time(19, 26, 6)),

		AllTypes.Time.IS_NOT_DISTINCT_FROM(AllTypes.Time),
		AllTypes.Time.IS_NOT_DISTINCT_FROM(Time(18, 36, 6)),

		AllTypes.Time.LT(AllTypes.Time),
		AllTypes.Time.LT(Time(17, 46, 6)),

		AllTypes.Time.LT_EQ(AllTypes.Time),
		AllTypes.Time.LT_EQ(Time(16, 56, 56)),

		AllTypes.Time.GT(AllTypes.Time),
		AllTypes.Time.GT(Time(15, 16, 46)),

		AllTypes.Time.GT_EQ(AllTypes.Time),
		AllTypes.Time.GT_EQ(Time(14, 26, 36)),

		AllTypes.Time.ADD(INTERVAL(10, MINUTE)),
		AllTypes.Time.ADD(INTERVALe(AllTypes.Integer, MINUTE)),
		AllTypes.Time.ADD(INTERVALd(3*time.Hour)),

		AllTypes.Time.SUB(INTERVAL(20, MINUTE)),
		AllTypes.Time.SUB(INTERVALe(AllTypes.SmallInt, MINUTE)),
		AllTypes.Time.SUB(INTERVALd(3*time.Minute)),

		AllTypes.Time.ADD(INTERVAL(20, MINUTE)).SUB(INTERVAL(11, HOUR)),

		CURRENT_TIME(),
		CURRENT_TIME(3),
	)

	//fmt.Println(query.DebugSql())

	testutils.AssertDebugStatementSql(t, query, strings.Replace(`
SELECT CAST('20:34:58' AS TIME),
     all_types.time = all_types.time,
     all_types.time = CAST('23:06:06' AS TIME),
     all_types.time = CAST('22:06:06.011' AS TIME),
     all_types.time = CAST('21:06:06.011111' AS TIME),
     all_types.time_ptr != all_types.time,
     all_types.time_ptr != CAST('20:16:06' AS TIME),
     NOT(all_types.time <=> all_types.time),
     NOT(all_types.time <=> CAST('19:26:06' AS TIME)),
     all_types.time <=> all_types.time,
     all_types.time <=> CAST('18:36:06' AS TIME),
     all_types.time < all_types.time,
     all_types.time < CAST('17:46:06' AS TIME),
     all_types.time <= all_types.time,
     all_types.time <= CAST('16:56:56' AS TIME),
     all_types.time > all_types.time,
     all_types.time > CAST('15:16:46' AS TIME),
     all_types.time >= all_types.time,
     all_types.time >= CAST('14:26:36' AS TIME),
     all_types.time + INTERVAL 10 MINUTE,
     all_types.time + INTERVAL all_types.''integer'' MINUTE,
     all_types.time + INTERVAL 3 HOUR,
     all_types.time - INTERVAL 20 MINUTE,
     all_types.time - INTERVAL all_types.small_int MINUTE,
     all_types.time - INTERVAL 3 MINUTE,
     (all_types.time + INTERVAL 20 MINUTE) - INTERVAL 11 HOUR,
     CURRENT_TIME,
     CURRENT_TIME(3)
FROM test_sample.all_types;
`, "''", "`", -1), "20:34:58", "23:06:06", "22:06:06.011", "21:06:06.011111", "20:16:06",
		"19:26:06", "18:36:06", "17:46:06", "16:56:56", "15:16:46", "14:26:36")

	dest := []struct{}{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestDateExpressions(t *testing.T) {
	query := AllTypes.SELECT(
		Date(timeT.Date()),

		AllTypes.Date.EQ(AllTypes.Date),
		AllTypes.Date.EQ(Date(2019, 6, 6)),

		AllTypes.DatePtr.NOT_EQ(AllTypes.Date),
		AllTypes.DatePtr.NOT_EQ(Date(2019, 1, 6)),

		AllTypes.Date.IS_DISTINCT_FROM(AllTypes.Date),
		AllTypes.Date.IS_DISTINCT_FROM(Date(2019, 2, 6)),

		AllTypes.Date.IS_NOT_DISTINCT_FROM(AllTypes.Date),
		AllTypes.Date.IS_NOT_DISTINCT_FROM(Date(2019, 3, 6)),

		AllTypes.Date.LT(AllTypes.Date),
		AllTypes.Date.LT(Date(2019, 4, 6)),

		AllTypes.Date.LT_EQ(AllTypes.Date),
		AllTypes.Date.LT_EQ(Date(2019, 5, 5)),

		AllTypes.Date.GT(AllTypes.Date),
		AllTypes.Date.GT(Date(2019, 1, 4)),

		AllTypes.Date.GT_EQ(AllTypes.Date),
		AllTypes.Date.GT_EQ(Date(2019, 2, 3)),

		AllTypes.Date.ADD(INTERVAL("10:20.000100", MINUTE_MICROSECOND)),
		AllTypes.Date.ADD(INTERVALe(AllTypes.BigInt, MINUTE)),
		AllTypes.Date.ADD(INTERVALd(15*time.Hour)),

		AllTypes.Date.SUB(INTERVAL(20, MINUTE)),
		AllTypes.Date.SUB(INTERVALe(AllTypes.SmallInt, MINUTE)),
		AllTypes.Date.SUB(INTERVALd(3*time.Minute)),

		CURRENT_DATE(),
	)

	//fmt.Println(query.DebugSql())

	testutils.AssertDebugStatementSql(t, query, `
SELECT CAST('2009-11-17' AS DATE),
     all_types.date = all_types.date,
     all_types.date = CAST('2019-06-06' AS DATE),
     all_types.date_ptr != all_types.date,
     all_types.date_ptr != CAST('2019-01-06' AS DATE),
     NOT(all_types.date <=> all_types.date),
     NOT(all_types.date <=> CAST('2019-02-06' AS DATE)),
     all_types.date <=> all_types.date,
     all_types.date <=> CAST('2019-03-06' AS DATE),
     all_types.date < all_types.date,
     all_types.date < CAST('2019-04-06' AS DATE),
     all_types.date <= all_types.date,
     all_types.date <= CAST('2019-05-05' AS DATE),
     all_types.date > all_types.date,
     all_types.date > CAST('2019-01-04' AS DATE),
     all_types.date >= all_types.date,
     all_types.date >= CAST('2019-02-03' AS DATE),
     all_types.date + INTERVAL '10:20.000100' MINUTE_MICROSECOND,
     all_types.date + INTERVAL all_types.big_int MINUTE,
     all_types.date + INTERVAL 15 HOUR,
     all_types.date - INTERVAL 20 MINUTE,
     all_types.date - INTERVAL all_types.small_int MINUTE,
     all_types.date - INTERVAL 3 MINUTE,
     CURRENT_DATE
FROM test_sample.all_types;
`)

	dest := []struct{}{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestDateTimeExpressions(t *testing.T) {

	var dateTime = DateTime(2019, 6, 6, 10, 2, 46)

	query := AllTypes.SELECT(
		AllTypes.DateTime.EQ(AllTypes.DateTime),
		AllTypes.DateTime.EQ(dateTime),

		AllTypes.DateTimePtr.NOT_EQ(AllTypes.DateTime),
		AllTypes.DateTimePtr.NOT_EQ(DateTime(2019, 6, 6, 10, 2, 46, 100*time.Millisecond)),

		AllTypes.DateTime.IS_DISTINCT_FROM(AllTypes.DateTime),
		AllTypes.DateTime.IS_DISTINCT_FROM(dateTime),

		AllTypes.DateTime.IS_NOT_DISTINCT_FROM(AllTypes.DateTime),
		AllTypes.DateTime.IS_NOT_DISTINCT_FROM(dateTime),

		AllTypes.DateTime.LT(AllTypes.DateTime),
		AllTypes.DateTime.LT(dateTime),

		AllTypes.DateTime.LT_EQ(AllTypes.DateTime),
		AllTypes.DateTime.LT_EQ(dateTime),

		AllTypes.DateTime.GT(AllTypes.DateTime),
		AllTypes.DateTime.GT(dateTime),

		AllTypes.DateTime.GT_EQ(AllTypes.DateTime),
		AllTypes.DateTime.GT_EQ(dateTime),

		AllTypes.DateTime.ADD(INTERVAL("05:10:20.000100", HOUR_MICROSECOND)),
		AllTypes.DateTime.ADD(INTERVALe(AllTypes.BigInt, HOUR)),
		AllTypes.DateTime.ADD(INTERVALd(2*time.Hour)),

		AllTypes.DateTime.SUB(INTERVAL("05:10:20.000100", HOUR_MICROSECOND)),
		AllTypes.DateTime.SUB(INTERVALe(AllTypes.IntegerPtr, HOUR)),
		AllTypes.DateTime.SUB(INTERVALd(3*time.Hour)),

		NOW(),
		NOW(1),
	)

	//Println(query.DebugSql())

	testutils.AssertDebugStatementSql(t, query, `
SELECT all_types.date_time = all_types.date_time,
     all_types.date_time = CAST('2019-06-06 10:02:46' AS DATETIME),
     all_types.date_time_ptr != all_types.date_time,
     all_types.date_time_ptr != CAST('2019-06-06 10:02:46.100' AS DATETIME),
     NOT(all_types.date_time <=> all_types.date_time),
     NOT(all_types.date_time <=> CAST('2019-06-06 10:02:46' AS DATETIME)),
     all_types.date_time <=> all_types.date_time,
     all_types.date_time <=> CAST('2019-06-06 10:02:46' AS DATETIME),
     all_types.date_time < all_types.date_time,
     all_types.date_time < CAST('2019-06-06 10:02:46' AS DATETIME),
     all_types.date_time <= all_types.date_time,
     all_types.date_time <= CAST('2019-06-06 10:02:46' AS DATETIME),
     all_types.date_time > all_types.date_time,
     all_types.date_time > CAST('2019-06-06 10:02:46' AS DATETIME),
     all_types.date_time >= all_types.date_time,
     all_types.date_time >= CAST('2019-06-06 10:02:46' AS DATETIME),
     all_types.date_time + INTERVAL '05:10:20.000100' HOUR_MICROSECOND,
     all_types.date_time + INTERVAL all_types.big_int HOUR,
     all_types.date_time + INTERVAL 2 HOUR,
     all_types.date_time - INTERVAL '05:10:20.000100' HOUR_MICROSECOND,
     all_types.date_time - INTERVAL all_types.integer_ptr HOUR,
     all_types.date_time - INTERVAL 3 HOUR,
     NOW(),
     NOW(1)
FROM test_sample.all_types;
`)

	dest := []struct{}{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestTimestampExpressions(t *testing.T) {

	var timestamp = Timestamp(2019, 6, 6, 10, 2, 46)

	query := AllTypes.SELECT(
		AllTypes.Timestamp.EQ(AllTypes.Timestamp),
		AllTypes.Timestamp.EQ(timestamp),

		AllTypes.TimestampPtr.NOT_EQ(AllTypes.Timestamp),
		AllTypes.TimestampPtr.NOT_EQ(Timestamp(2019, 6, 6, 10, 2, 46, 100*time.Millisecond)),

		AllTypes.Timestamp.IS_DISTINCT_FROM(AllTypes.Timestamp),
		AllTypes.Timestamp.IS_DISTINCT_FROM(timestamp),

		AllTypes.Timestamp.IS_NOT_DISTINCT_FROM(AllTypes.Timestamp),
		AllTypes.Timestamp.IS_NOT_DISTINCT_FROM(timestamp),

		AllTypes.Timestamp.LT(AllTypes.Timestamp),
		AllTypes.Timestamp.LT(timestamp),

		AllTypes.Timestamp.LT_EQ(AllTypes.Timestamp),
		AllTypes.Timestamp.LT_EQ(timestamp),

		AllTypes.Timestamp.GT(AllTypes.Timestamp),
		AllTypes.Timestamp.GT(timestamp),

		AllTypes.Timestamp.GT_EQ(AllTypes.Timestamp),
		AllTypes.Timestamp.GT_EQ(timestamp),

		AllTypes.Timestamp.ADD(INTERVAL("05:10:20.000100", HOUR_MICROSECOND)),
		AllTypes.Timestamp.ADD(INTERVALe(AllTypes.BigInt, HOUR)),
		AllTypes.Timestamp.ADD(INTERVALd(2*time.Hour)),

		AllTypes.Timestamp.SUB(INTERVAL("05:10:20.000100", HOUR_MICROSECOND)),
		AllTypes.Timestamp.SUB(INTERVALe(AllTypes.IntegerPtr, HOUR)),
		AllTypes.Timestamp.SUB(INTERVALd(3*time.Hour)),

		CURRENT_TIMESTAMP(),
		CURRENT_TIMESTAMP(2),
	)

	//fmt.Println(query.DebugSql())

	testutils.AssertDebugStatementSql(t, query, `
SELECT all_types.timestamp = all_types.timestamp,
     all_types.timestamp = TIMESTAMP('2019-06-06 10:02:46'),
     all_types.timestamp_ptr != all_types.timestamp,
     all_types.timestamp_ptr != TIMESTAMP('2019-06-06 10:02:46.100'),
     NOT(all_types.timestamp <=> all_types.timestamp),
     NOT(all_types.timestamp <=> TIMESTAMP('2019-06-06 10:02:46')),
     all_types.timestamp <=> all_types.timestamp,
     all_types.timestamp <=> TIMESTAMP('2019-06-06 10:02:46'),
     all_types.timestamp < all_types.timestamp,
     all_types.timestamp < TIMESTAMP('2019-06-06 10:02:46'),
     all_types.timestamp <= all_types.timestamp,
     all_types.timestamp <= TIMESTAMP('2019-06-06 10:02:46'),
     all_types.timestamp > all_types.timestamp,
     all_types.timestamp > TIMESTAMP('2019-06-06 10:02:46'),
     all_types.timestamp >= all_types.timestamp,
     all_types.timestamp >= TIMESTAMP('2019-06-06 10:02:46'),
     all_types.timestamp + INTERVAL '05:10:20.000100' HOUR_MICROSECOND,
     all_types.timestamp + INTERVAL all_types.big_int HOUR,
     all_types.timestamp + INTERVAL 2 HOUR,
     all_types.timestamp - INTERVAL '05:10:20.000100' HOUR_MICROSECOND,
     all_types.timestamp - INTERVAL all_types.integer_ptr HOUR,
     all_types.timestamp - INTERVAL 3 HOUR,
     CURRENT_TIMESTAMP,
     CURRENT_TIMESTAMP(2)
FROM test_sample.all_types;
`)
	dest := []struct{}{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestTimeLiterals(t *testing.T) {

	loc, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	var timeT = time.Date(2009, 11, 17, 20, 34, 58, 351387237, loc)

	query := SELECT(
		Date(timeT.Date()).AS("date"),
		DateT(timeT).AS("dateT"),
		Time(timeT.Clock()).AS("time"),
		TimeT(timeT).AS("timeT"),
		DateTimeT(timeT).AS("datetime"),
		Timestamp(2019, 8, 6, 10, 10, 30, 123456*time.Millisecond).AS("timestamp"),
		TimestampT(timeT).AS("timestampT"),
	).FROM(AllTypes).LIMIT(1)

	//fmt.Println(query.DebugSql())

	testutils.AssertStatementSql(t, query, `
SELECT CAST(? AS DATE) AS "date",
     CAST(? AS DATE) AS "dateT",
     CAST(? AS TIME) AS "time",
     CAST(? AS TIME) AS "timeT",
     CAST(? AS DATETIME) AS "datetime",
     TIMESTAMP(?) AS "timestamp",
     TIMESTAMP(?) AS "timestampT"
FROM test_sample.all_types
LIMIT ?;
`)

	var dest struct {
		Date       time.Time
		DateT      time.Time
		Time       time.Time
		TimeT      time.Time
		DateTime   time.Time
		Timestamp  time.Time
		TimestampT time.Time
	}

	err = query.Query(db, &dest)
	require.NoError(t, err)

	//testutils.PrintJson(dest)

	if sourceIsMariaDB() {
		testutils.AssertJSON(t, dest, `
{
	"Date": "2009-11-17T00:00:00Z",
	"DateT": "2009-11-17T00:00:00Z",
	"Time": "0000-01-01T20:34:58Z",
	"TimeT": "0000-01-01T19:34:58Z",
	"DateTime": "2009-11-17T19:34:58Z",
	"Timestamp": "2019-08-06T10:10:30Z",
	"TimestampT": "2009-11-17T19:34:58Z"
}
`)
	} else {
		testutils.AssertJSON(t, dest, `
{
	"Date": "2009-11-17T00:00:00Z",
	"DateT": "2009-11-17T00:00:00Z",
	"Time": "0000-01-01T20:34:58Z",
	"TimeT": "0000-01-01T19:34:58Z",
	"DateTime": "2009-11-17T19:34:58Z",
	"Timestamp": "2019-08-06T10:10:30.123456Z",
	"TimestampT": "2009-11-17T19:34:58.351387Z"
}
`)
	}

}

func TestINTERVAL(t *testing.T) {
	query := SELECT(
		Date(2000, 2, 10).ADD(INTERVAL(1, MICROSECOND)).
			EQ(Timestamp(2000, 2, 10, 0, 0, 0, 1*time.Microsecond)),
		Date(2000, 2, 10).SUB(INTERVAL(2, SECOND)),
		Date(2000, 2, 10).ADD(INTERVAL(3, MINUTE)),
		Date(2000, 2, 10).SUB(INTERVAL(4, HOUR)),
		Date(2000, 2, 10).ADD(INTERVAL(5, DAY)),
		Date(2000, 2, 10).SUB(INTERVAL(6, MONTH)),
		Date(2000, 2, 10).ADD(INTERVAL(7, YEAR)),
		Date(2000, 2, 10).ADD(INTERVAL(-7, YEAR)),
		Date(2000, 2, 10).ADD(INTERVAL("20.0000100", SECOND_MICROSECOND)),
		Date(2000, 2, 10).SUB(INTERVAL("02:20.0000100", MINUTE_MICROSECOND)),
		Date(2000, 2, 10).SUB(INTERVAL("11:02:20.0000100", HOUR_MICROSECOND)),
		Date(2000, 2, 10).SUB(INTERVAL("100 11:02:20.0000100", DAY_MICROSECOND)),
		Date(2000, 2, 10).SUB(INTERVAL("11:02", MINUTE_SECOND)),
		Date(2000, 2, 10).SUB(INTERVAL("11:02:20", HOUR_SECOND)),
		Date(2000, 2, 10).SUB(INTERVAL("11:02", HOUR_MINUTE)),
		Date(2000, 2, 10).SUB(INTERVAL("11 02:03:04", DAY_SECOND)),
		Date(2000, 2, 10).SUB(INTERVAL("11 02:03", DAY_MINUTE)),
		Date(2000, 2, 10).SUB(INTERVAL("11 2", DAY_HOUR)),
		Date(2000, 2, 10).SUB(INTERVAL("2000-2", YEAR_MONTH)),

		Date(2000, 2, 10).SUB(INTERVALe(AllTypes.IntegerPtr, MICROSECOND)),
		Date(2000, 2, 10).SUB(INTERVALe(AllTypes.IntegerPtr, SECOND)),
		Date(2000, 2, 10).SUB(INTERVALe(AllTypes.IntegerPtr, MINUTE)),
		Date(2000, 2, 10).SUB(INTERVALe(AllTypes.IntegerPtr, HOUR)),
		Date(2000, 2, 10).SUB(INTERVALe(AllTypes.IntegerPtr, DAY)),
		Date(2000, 2, 10).SUB(INTERVALe(AllTypes.IntegerPtr, WEEK)),
		Date(2000, 2, 10).SUB(INTERVALe(AllTypes.IntegerPtr, MONTH)),
		Date(2000, 2, 10).SUB(INTERVALe(AllTypes.IntegerPtr, QUARTER)),
		Date(2000, 2, 10).SUB(INTERVALe(AllTypes.IntegerPtr, YEAR)),

		Date(2000, 2, 10).SUB(INTERVALd(3*time.Microsecond)),
		Date(2000, 2, 10).SUB(INTERVALd(-3*time.Microsecond)),
		Date(2000, 2, 10).SUB(INTERVALd(3*time.Second)),
		Date(2000, 2, 10).SUB(INTERVALd(3*time.Second+4*time.Microsecond)),
		Date(2000, 2, 10).SUB(INTERVALd(3*time.Minute+4*time.Second+5*time.Microsecond)),
		Date(2000, 2, 10).SUB(INTERVALd(3*time.Hour+4*time.Minute+5*time.Second+6*time.Microsecond)),
		Date(2000, 2, 10).SUB(INTERVALd(2*24*time.Hour+3*time.Hour+4*time.Minute+5*time.Second+6*time.Microsecond)),
		Date(2000, 2, 10).SUB(INTERVALd(2*24*time.Hour+3*time.Hour+4*time.Minute+5*time.Second)),
		Date(2000, 2, 10).SUB(INTERVALd(2*24*time.Hour+3*time.Hour+4*time.Minute)),
		Date(2000, 2, 10).SUB(INTERVALd(2*24*time.Hour+3*time.Hour)),
		Date(2000, 2, 10).SUB(INTERVALd(2*24*time.Hour)),
		Date(2000, 2, 10).SUB(INTERVALd(3*time.Hour)),
		Date(2000, 2, 10).SUB(INTERVALd(1*time.Hour+2*time.Minute+3*time.Second+345*time.Microsecond)),
	).FROM(AllTypes)

	//fmt.Println(query.DebugSql())

	err := query.Query(db, &struct{}{})
	require.NoError(t, err)
}

func TestAllTypesInsert(t *testing.T) {
	tx, err := db.Begin()
	require.NoError(t, err)

	stmt := AllTypes.INSERT(AllTypes.AllColumns).
		MODEL(toInsert)

	fmt.Println(stmt.DebugSql())

	testutils.AssertExec(t, stmt, tx, 1)

	var dest model.AllTypes
	err = AllTypes.SELECT(AllTypes.AllColumns).
		WHERE(AllTypes.BigInt.EQ(Int(toInsert.BigInt))).
		Query(tx, &dest)

	require.NoError(t, err)
	require.Equal(t, toInsert.TinyInt, dest.TinyInt)

	err = tx.Rollback()
	require.NoError(t, err)
}

func TestAllTypesInsertOnDuplicateKeyUpdate(t *testing.T) {
	tx, err := db.Begin()
	require.NoError(t, err)

	toInsert := model.AllTypes{
		Boolean:   true,
		Integer:   124,
		Float:     45.67,
		Blob:      []byte("blob"),
		Text:      "text",
		JSON:      "{}",
		Time:      time.Now(),
		Timestamp: time.Now(),
		Date:      time.Now(),
	}

	stmt := AllTypes.INSERT(
		AllTypes.Boolean,
		AllTypes.Integer,
		AllTypes.Float,
		AllTypes.Blob,
		AllTypes.Text,
		AllTypes.JSON,
		AllTypes.Time,
		AllTypes.Timestamp,
		AllTypes.Date,
	).
		MODEL(toInsert).
		ON_DUPLICATE_KEY_UPDATE(
			AllTypes.Boolean.SET(Bool(false)),
			AllTypes.Integer.SET(Int(4)),
			AllTypes.Float.SET(Float(0.67)),
			AllTypes.Text.SET(String("new text")),
			AllTypes.Time.SET(TimeT(time.Now())),
			AllTypes.Timestamp.SET(TimestampT(time.Now())),
			AllTypes.Date.SET(DateT(time.Now())),
		)

	fmt.Println(stmt.DebugSql())

	_, err = stmt.Exec(tx)
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)
}

var toInsert = model.AllTypes{
	Boolean:       false,
	BooleanPtr:    testutils.BoolPtr(true),
	TinyInt:       1,
	UTinyInt:      2,
	SmallInt:      3,
	USmallInt:     4,
	MediumInt:     5,
	UMediumInt:    6,
	Integer:       7,
	UInteger:      8,
	BigInt:        9,
	UBigInt:       1122334455,
	TinyIntPtr:    testutils.Int8Ptr(11),
	UTinyIntPtr:   testutils.UInt8Ptr(22),
	SmallIntPtr:   testutils.Int16Ptr(33),
	USmallIntPtr:  testutils.UInt16Ptr(44),
	MediumIntPtr:  testutils.Int32Ptr(55),
	UMediumIntPtr: testutils.UInt32Ptr(66),
	IntegerPtr:    testutils.Int32Ptr(77),
	UIntegerPtr:   testutils.UInt32Ptr(88),
	BigIntPtr:     testutils.Int64Ptr(99),
	UBigIntPtr:    testutils.UInt64Ptr(111),
	Decimal:       11.22,
	DecimalPtr:    testutils.Float64Ptr(33.44),
	Numeric:       55.66,
	NumericPtr:    testutils.Float64Ptr(77.88),
	Float:         99.00,
	FloatPtr:      testutils.Float64Ptr(11.22),
	Double:        33.44,
	DoublePtr:     testutils.Float64Ptr(55.66),
	Real:          77.88,
	RealPtr:       testutils.Float64Ptr(99.00),
	Bit:           "1",
	BitPtr:        testutils.StringPtr("0"),
	Time:          time.Date(0, 0, 0, 10, 11, 12, 100, &time.Location{}),
	TimePtr:       testutils.TimePtr(time.Date(0, 0, 0, 10, 11, 12, 100, time.UTC)),
	Date:          time.Now(),
	DatePtr:       testutils.TimePtr(time.Now()),
	DateTime:      time.Now(),
	DateTimePtr:   testutils.TimePtr(time.Now()),
	Timestamp:     time.Now(),
	//TimestampPtr:  testutils.TimePtr(time.Now()), // TODO: build fails for MariaDB
	Year:         2000,
	YearPtr:      testutils.Int16Ptr(2001),
	Char:         "abcd",
	CharPtr:      testutils.StringPtr("absd"),
	VarChar:      "abcd",
	VarCharPtr:   testutils.StringPtr("absd"),
	Binary:       []byte("1010"),
	BinaryPtr:    testutils.ByteArrayPtr([]byte("100001")),
	VarBinary:    []byte("1010"),
	VarBinaryPtr: testutils.ByteArrayPtr([]byte("100001")),
	Blob:         []byte("large file"),
	BlobPtr:      testutils.ByteArrayPtr([]byte("very large file")),
	Text:         "some text",
	TextPtr:      testutils.StringPtr("text"),
	Enum:         model.AllTypesEnum_Value1,
	JSON:         "{}",
	JSONPtr:      testutils.StringPtr(`{"a": 1}`),
}

var allTypesJson = `
[
	{
		"Boolean": false,
		"BooleanPtr": true,
		"TinyInt": -3,
		"UTinyInt": 3,
		"SmallInt": 14,
		"USmallInt": 14,
		"MediumInt": -150,
		"UMediumInt": 150,
		"Integer": -1600,
		"UInteger": 1600,
		"BigInt": 5000,
		"UBigInt": 50000,
		"TinyIntPtr": -3,
		"UTinyIntPtr": 3,
		"SmallIntPtr": 14,
		"USmallIntPtr": 14,
		"MediumIntPtr": -150,
		"UMediumIntPtr": 150,
		"IntegerPtr": -1600,
		"UIntegerPtr": 1600,
		"BigIntPtr": 50000,
		"UBigIntPtr": 50000,
		"Decimal": 1.11,
		"DecimalPtr": 1.11,
		"Numeric": 2.22,
		"NumericPtr": 2.22,
		"Float": 3.33,
		"FloatPtr": 3.33,
		"Double": 4.44,
		"DoublePtr": 4.44,
		"Real": 5.55,
		"RealPtr": 5.55,
		"Bit": "\u0000\u0003",
		"BitPtr": "\u0000\u0003",
		"Time": "0000-01-01T10:11:12Z",
		"TimePtr": "0000-01-01T10:11:12Z",
		"Date": "2008-07-04T00:00:00Z",
		"DatePtr": "2008-07-04T00:00:00Z",
		"DateTime": "2011-12-18T13:17:17Z",
		"DateTimePtr": "2011-12-18T13:17:17Z",
		"Timestamp": "2007-12-31T23:00:01Z",
		"TimestampPtr": "2007-12-31T23:00:01Z",
		"Year": 2004,
		"YearPtr": 2004,
		"Char": "char",
		"CharPtr": "char",
		"VarChar": "varchar",
		"VarCharPtr": "varchar",
		"Binary": "YmluYXJ5AAAAAAAAAAAAAAAAAAA=",
		"BinaryPtr": "YmluYXJ5AAAAAAAAAAAAAAAAAAA=",
		"VarBinary": "dmFyYmluYXJ5",
		"VarBinaryPtr": "dmFyYmluYXJ5",
		"Blob": "YmxvYg==",
		"BlobPtr": "YmxvYg==",
		"Text": "text",
		"TextPtr": "text",
		"Enum": "value1",
		"EnumPtr": "value1",
		"Set": "s1",
		"SetPtr": "s2",
		"JSON": "{\"key1\": \"value1\", \"key2\": \"value2\"}",
		"JSONPtr": "{\"key1\": \"value1\", \"key2\": \"value2\"}"
	},
	{
		"Boolean": false,
		"BooleanPtr": null,
		"TinyInt": -3,
		"UTinyInt": 3,
		"SmallInt": 14,
		"USmallInt": 14,
		"MediumInt": -150,
		"UMediumInt": 150,
		"Integer": -1600,
		"UInteger": 1600,
		"BigInt": 5000,
		"UBigInt": 50000,
		"TinyIntPtr": null,
		"UTinyIntPtr": null,
		"SmallIntPtr": null,
		"USmallIntPtr": null,
		"MediumIntPtr": null,
		"UMediumIntPtr": null,
		"IntegerPtr": null,
		"UIntegerPtr": null,
		"BigIntPtr": null,
		"UBigIntPtr": null,
		"Decimal": 1.11,
		"DecimalPtr": null,
		"Numeric": 2.22,
		"NumericPtr": null,
		"Float": 3.33,
		"FloatPtr": null,
		"Double": 4.44,
		"DoublePtr": null,
		"Real": 5.55,
		"RealPtr": null,
		"Bit": "\u0000\u0003",
		"BitPtr": null,
		"Time": "0000-01-01T10:11:12Z",
		"TimePtr": null,
		"Date": "2008-07-04T00:00:00Z",
		"DatePtr": null,
		"DateTime": "2011-12-18T13:17:17Z",
		"DateTimePtr": null,
		"Timestamp": "2007-12-31T23:00:01Z",
		"TimestampPtr": null,
		"Year": 2004,
		"YearPtr": null,
		"Char": "char",
		"CharPtr": null,
		"VarChar": "varchar",
		"VarCharPtr": null,
		"Binary": "YmluYXJ5AAAAAAAAAAAAAAAAAAA=",
		"BinaryPtr": null,
		"VarBinary": "dmFyYmluYXJ5",
		"VarBinaryPtr": null,
		"Blob": "YmxvYg==",
		"BlobPtr": null,
		"Text": "text",
		"TextPtr": null,
		"Enum": "value1",
		"EnumPtr": null,
		"Set": "s1",
		"SetPtr": null,
		"JSON": "{\"key1\": \"value1\", \"key2\": \"value2\"}",
		"JSONPtr": null
	}
]
`

func TestReservedWord(t *testing.T) {
	stmt := SELECT(User.AllColumns).
		FROM(User)

	testutils.AssertDebugStatementSql(t, stmt, strings.Replace(`
SELECT user.''column'' AS "user.column",
     user.''use'' AS "user.use",
     user.ceil AS "user.ceil",
     user.commit AS "user.commit",
     user.''create'' AS "user.create",
     user.''default'' AS "user.default",
     user.''desc'' AS "user.desc",
     user.''empty'' AS "user.empty",
     user.''float'' AS "user.float",
     user.''join'' AS "user.join",
     user.''like'' AS "user.like",
     user.max AS "user.max",
     user.''rank'' AS "user.rank"
FROM test_sample.user;
`, "''", "`", -1))

	var dest []model.User
	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	testutils.PrintJson(dest)

	testutils.AssertJSON(t, dest, `
[
	{
		"Column": "Column",
		"Use": "CHECK",
		"Ceil": "CEIL",
		"Commit": "COMMIT",
		"Create": "CREATE",
		"Default": "DEFAULT",
		"Desc": "DESC",
		"Empty": "EMPTY",
		"Float": "FLOAT",
		"Join": "JOIN",
		"Like": "LIKE",
		"Max": "MAX",
		"Rank": "RANK"
	}
]
`)
}
