package mysql

import (
	"github.com/go-jet/jet/internal/testutils"
	"github.com/go-jet/jet/tests/.gentestdata/mysql/test_sample/model"
	. "github.com/go-jet/jet/tests/.gentestdata/mysql/test_sample/table"
	"github.com/go-jet/jet/tests/testdata/common"

	. "github.com/go-jet/jet/mysql"

	"gotest.tools/assert"
	"testing"
)

func TestAllTypes(t *testing.T) {

	dest := []model.AllTypes{}

	err := AllTypes.
		SELECT(AllTypes.AllColumns).
		LIMIT(2).
		Query(db, &dest)

	assert.NilError(t, err)

	//testutils.JsonPrint(dest)
	testutils.AssertJSON(t, dest, allTypesJson)
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
     (NOT all_types.boolean <=> all_types.boolean_ptr) AS "distinct1",
     (NOT all_types.boolean <=> ?) AS "distinct2",
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

	assert.NilError(t, err)

	testutils.AssertJSONFile(t, "./testdata/common/bool_operators.json", dest)
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

	queryStr, _, err := query.Sql()
	assert.NilError(t, err)
	assert.Equal(t, queryStr, `
SELECT (all_types.numeric = all_types.numeric) AS "eq1",
     (all_types.decimal = ?) AS "eq2",
     (all_types.real = ?) AS "eq3",
     (NOT all_types.numeric <=> all_types.numeric) AS "distinct1",
     (NOT all_types.decimal <=> ?) AS "distinct2",
     (NOT all_types.real <=> ?) AS "distinct3",
     (all_types.numeric <=> all_types.numeric) AS "not_distinct1",
     (all_types.decimal <=> ?) AS "not_distinct2",
     (all_types.real <=> ?) AS "not_distinct3",
     (all_types.numeric < ?) AS "lt1",
     (all_types.numeric < ?) AS "lt2",
     (all_types.numeric > ?) AS "gt1",
     (all_types.numeric > ?) AS "gt2",
     TRUNCATE((all_types.decimal + all_types.decimal), ?) AS "add1",
     TRUNCATE((all_types.decimal + ?), ?) AS "add2",
     TRUNCATE((all_types.decimal - all_types.decimal_ptr), ?) AS "sub1",
     TRUNCATE((all_types.decimal - ?), ?) AS "sub2",
     TRUNCATE((all_types.decimal * all_types.decimal_ptr), ?) AS "mul1",
     TRUNCATE((all_types.decimal * ?), ?) AS "mul2",
     TRUNCATE((all_types.decimal / all_types.decimal_ptr), ?) AS "div1",
     TRUNCATE((all_types.decimal / ?), ?) AS "div2",
     TRUNCATE((all_types.decimal % all_types.decimal_ptr), ?) AS "mod1",
     TRUNCATE((all_types.decimal % ?), ?) AS "mod2",
     TRUNCATE(POW(all_types.decimal, all_types.decimal_ptr), ?) AS "pow1",
     TRUNCATE(POW(all_types.decimal, ?), ?) AS "pow2",
     TRUNCATE(ABS(all_types.decimal), ?) AS "abs",
     TRUNCATE(POWER(all_types.decimal, ?), ?) AS "power",
     TRUNCATE(SQRT(all_types.decimal), ?) AS "sqrt",
     TRUNCATE(POWER(all_types.decimal, (? / ?)), ?) AS "cbrt",
     CEIL(all_types.real) AS "ceil",
     FLOOR(all_types.real) AS "floor",
     ROUND(all_types.decimal) AS "round1",
     ROUND(all_types.decimal, ?) AS "round2",
     SIGN(all_types.real) AS "sign",
     TRUNCATE(all_types.decimal, ?) AS "trunc"
FROM test_sample.all_types
LIMIT ?;
`)

	var dest []struct {
		common.FloatExpressionTestResult `alias:"."`
	}

	err = query.Query(db, &dest)

	assert.NilError(t, err)

	testutils.AssertJSONFile(t, "./testdata/common/float_operators.json", dest)
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

		BIT_NOT(MINUSi(AllTypes.SmallInt)).AS("bit_not_1"),
		BIT_NOT(MINUSi(Int(11, true))).AS("bit_not_2"),

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
     (NOT all_types.big_int <=> all_types.big_int) AS "distinct1",
     (NOT all_types.big_int <=> ?) AS "distinct2",
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
     (~ (- all_types.small_int)) AS "bit_not_1",
     (~ (- 11)) AS "bit_not_2",
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

	assert.NilError(t, err)

	//testutils.JsonPrint(dest)

	testutils.AssertJSONFile(t, "./testdata/common/int_operators.json", dest)
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
		"Varchar": "varchar",
		"VarcharPtr": "varchar",
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
		"Varchar": "varchar",
		"VarcharPtr": null,
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
