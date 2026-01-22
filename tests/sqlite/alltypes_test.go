package sqlite

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/internal/utils/ptr"
	. "github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/test_sample/table"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/test_sample/view"
	"github.com/go-jet/jet/v2/tests/testdata/results/common"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestAllTypes(t *testing.T) {

	var dest []model.AllTypes

	err := SELECT(AllTypes.AllColumns).
		FROM(AllTypes).
		Query(sampleDB, &dest)

	require.NoError(t, err)
	testutils.AssertJSON(t, dest, allTypesJSON)
}

func TestAllTypesJSON(t *testing.T) {

	stmt := SELECT_JSON_ARR(
		AllTypes.AllColumns.Except(
			AllTypes.JSON,
			AllTypes.JSONPtr,
		),
		CAST(AllTypes.JSON).AS_TEXT().AS("Json"),
		CAST(AllTypes.JSONPtr).AS_TEXT().AS("JsonPtr"),
	).FROM(AllTypes)

	testutils.AssertStatementSql(t, stmt, strings.ReplaceAll(`
SELECT JSON_GROUP_ARRAY(JSON_OBJECT(
          'boolean', CASE all_types.boolean WHEN 1 THEN json('true') WHEN 0 THEN json('false') ELSE json('null') END,
          'booleanPtr', CASE all_types.boolean_ptr WHEN 1 THEN json('true') WHEN 0 THEN json('false') ELSE json('null') END,
          'tinyInt', all_types.tiny_int,
          'tinyIntPtr', all_types.tiny_int_ptr,
          'smallInt', all_types.small_int,
          'smallIntPtr', all_types.small_int_ptr,
          'mediumInt', all_types.medium_int,
          'mediumIntPtr', all_types.medium_int_ptr,
          'integer', all_types.integer,
          'integerPtr', all_types.integer_ptr,
          'bigInt', all_types.big_int,
          'bigIntPtr', all_types.big_int_ptr,
          'decimal', all_types.decimal,
          'decimalPtr', all_types.decimal_ptr,
          'numeric', all_types.numeric,
          'numericPtr', all_types.numeric_ptr,
          'float', all_types.float,
          'floatPtr', all_types.float_ptr,
          'double', all_types.double,
          'doublePtr', all_types.double_ptr,
          'real', all_types.real,
          'realPtr', all_types.real_ptr,
          'time', strftime('0000-01-01T%H:%M:%fZ', all_types.time),
          'timePtr', strftime('0000-01-01T%H:%M:%fZ', all_types.time_ptr),
          'date', strftime('%Y-%m-%dT00:00:00Z', all_types.date),
          'datePtr', strftime('%Y-%m-%dT00:00:00Z', all_types.date_ptr),
          'dateTime', strftime('%Y-%m-%dT%H:%M:%fZ', all_types.date_time),
          'dateTimePtr', strftime('%Y-%m-%dT%H:%M:%fZ', all_types.date_time_ptr),
          'timestamp', strftime('%Y-%m-%dT%H:%M:%fZ', all_types.timestamp),
          'timestampPtr', strftime('%Y-%m-%dT%H:%M:%fZ', all_types.timestamp_ptr),
          'char', all_types.char,
          'charPtr', all_types.char_ptr,
          'varChar', all_types.var_char,
          'varCharPtr', all_types.var_char_ptr,
          'text', all_types.text,
          'textPtr', all_types.text_ptr,
          'blob', BASE64_ENCODE(all_types.blob),
          'blobPtr', BASE64_ENCODE(all_types.blob_ptr),
          'Json', CAST(all_types.json AS TEXT),
          'JsonPtr', CAST(all_types.json_ptr AS TEXT)
     )) AS "json"
FROM all_types;
`, "''", "`"))

	var dest []model.AllTypes

	err := stmt.QueryContext(ctx, sampleDB, &dest)
	require.NoError(t, err)

	var expected []model.AllTypes
	err = json.Unmarshal([]byte(allTypesJSON), &expected)
	require.NoError(t, err)

	testutils.AssertDeepEqual(t, dest, expected, cmpopts.EquateApproxTime(time.Second))
}

var allTypesJSON = `
[
	{
		"Boolean": false,
		"BooleanPtr": true,
		"TinyInt": -3,
		"TinyIntPtr": 3,
		"SmallInt": 14,
		"SmallIntPtr": 14,
		"MediumInt": -150,
		"MediumIntPtr": 150,
		"Integer": -1600,
		"IntegerPtr": 1600,
		"BigInt": 5000,
		"BigIntPtr": 50000,
		"Decimal": 1.11,
		"DecimalPtr": 1.01,
		"Numeric": 2.22,
		"NumericPtr": 2.02,
		"Float": 3.33,
		"FloatPtr": 3.03,
		"Double": 4.44,
		"DoublePtr": 4.04,
		"Real": 5.55,
		"RealPtr": 5.05,
		"Time": "0000-01-01T10:11:12.33Z",
		"TimePtr": "0000-01-01T10:11:12.123456Z",
		"Date": "2008-07-04T00:00:00Z",
		"DatePtr": "2008-07-04T00:00:00Z",
		"DateTime": "2011-12-18T13:17:17Z",
		"DateTimePtr": "2011-12-18T13:17:17Z",
		"Timestamp": "2007-12-31T23:00:01Z",
		"TimestampPtr": "2007-12-31T23:00:01Z",
		"Char": "char1",
		"CharPtr": "char-ptr",
		"VarChar": "varchar",
		"VarCharPtr": "varchar-ptr",
		"Text": "text",
		"TextPtr": "text-ptr",
		"Blob": "YmxvYjE=",
		"BlobPtr": "YmxvYi1wdHI=",
		"JSON": "{\"key1\": \"value1\", \"key2\": \"value2\"}",
		"JSONPtr": "{\"key1\": \"value1\", \"key2\": \"value2\"}"
	},
	{
		"Boolean": false,
		"BooleanPtr": null,
		"TinyInt": -3,
		"TinyIntPtr": null,
		"SmallInt": 14,
		"SmallIntPtr": null,
		"MediumInt": -150,
		"MediumIntPtr": null,
		"Integer": -1600,
		"IntegerPtr": null,
		"BigInt": 5000,
		"BigIntPtr": null,
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
		"Time": "0000-01-01T10:11:12.33Z",
		"TimePtr": null,
		"Date": "2008-07-04T00:00:00Z",
		"DatePtr": null,
		"DateTime": "2011-12-18T13:17:17Z",
		"DateTimePtr": null,
		"Timestamp": "2007-12-31T23:00:01Z",
		"TimestampPtr": null,
		"Char": "char2",
		"CharPtr": null,
		"VarChar": "varchar",
		"VarCharPtr": null,
		"Text": "text",
		"TextPtr": null,
		"Blob": "YmxvYjI=",
		"BlobPtr": null,
		"JSON": "{\"key1\": \"value1\", \"key2\": \"value2\"}",
		"JSONPtr": null
	}
]
`

func TestAllTypesViewSelect(t *testing.T) {
	var dest []model.AllTypesView

	stmt := SELECT(view.AllTypesView.AllColumns).
		FROM(view.AllTypesView)

	err := stmt.Query(sampleDB, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 2)

	testutils.AssertJSON(t, dest, allTypesJSON)
}

func TestAllTypesInsert(t *testing.T) {
	tx := beginSampleDBTx(t)
	defer tx.Rollback()

	stmt := AllTypes.INSERT(AllTypes.AllColumns).
		MODEL(toInsert).
		RETURNING(AllTypes.AllColumns)

	var inserted model.AllTypes
	err := stmt.Query(tx, &inserted)

	require.NoError(t, err)
	testutils.AssertDeepEqual(t, toInsert, inserted, testutils.UnixTimeComparer)

	var dest model.AllTypes
	err = AllTypes.SELECT(AllTypes.AllColumns).
		WHERE(AllTypes.BigInt.EQ(Int(toInsert.BigInt))).
		Query(tx, &dest)

	require.NoError(t, err)
	testutils.AssertDeepEqual(t, dest, toInsert, testutils.UnixTimeComparer)

	require.NoError(t, err)
}

var toInsert = model.AllTypes{
	Boolean:      false,
	BooleanPtr:   ptr.Of(true),
	TinyInt:      1,
	SmallInt:     3,
	MediumInt:    5,
	Integer:      7,
	BigInt:       9,
	TinyIntPtr:   ptr.Of(int8(11)),
	SmallIntPtr:  ptr.Of(int16(33)),
	MediumIntPtr: ptr.Of(int32(55)),
	IntegerPtr:   ptr.Of(int32(77)),
	BigIntPtr:    ptr.Of(int64(99)),
	Decimal:      11.22,
	DecimalPtr:   ptr.Of(33.44),
	Numeric:      55.66,
	NumericPtr:   ptr.Of(77.88),
	Float:        99.00,
	FloatPtr:     ptr.Of(11.22),
	Double:       33.44,
	DoublePtr:    ptr.Of(55.66),
	Real:         77.88,
	RealPtr:      ptr.Of(float32(99.00)),
	Time:         time.Date(1, 1, 1, 1, 1, 1, 10, time.UTC),
	TimePtr:      ptr.Of(time.Date(2, 2, 2, 2, 2, 2, 200, time.UTC)),
	Date:         time.Now(),
	DatePtr:      ptr.Of(time.Now()),
	DateTime:     time.Now(),
	DateTimePtr:  ptr.Of(time.Now()),
	Timestamp:    time.Now(),
	TimestampPtr: ptr.Of(time.Now()),
	Char:         "abcd",
	CharPtr:      ptr.Of("absd"),
	VarChar:      "abcd",
	VarCharPtr:   ptr.Of("absd"),
	Blob:         []byte("large file"),
	BlobPtr:      ptr.Of([]byte("very large file")),
	Text:         "some text",
	TextPtr:      ptr.Of("text"),
}

func TestUUID(t *testing.T) {
	query := SELECT(
		//Raw("uuid()").AS("uuid"),
		String("dc8daae3-b83b-11e9-8eb4-98ded00c39c6").AS("str_uuid"),
	)

	var dest struct {
		UUID    uuid.UUID
		StrUUID *uuid.UUID
	}

	err := query.Query(sampleDB, &dest)

	require.NoError(t, err)
	require.Equal(t, dest.StrUUID.String(), "dc8daae3-b83b-11e9-8eb4-98ded00c39c6")
	requireLogged(t, query)
}

func TestExpressionOperators(t *testing.T) {
	query := SELECT(
		AllTypes.Integer.IS_NULL().AS("result.is_null"),
		AllTypes.DatePtr.IS_NOT_NULL().AS("result.is_not_null"),
		AllTypes.SmallIntPtr.IN(Int(11), Int(22)).AS("result.in"),
		AllTypes.SmallIntPtr.IN(AllTypes.SELECT(AllTypes.Integer)).AS("result.in_select"),

		Raw("length(121232459)").AS("result.raw"),
		Raw(":first + COALESCE(all_types.small_int_ptr, 0) + :second", RawArgs{":first": 78, ":second": 56}).
			AS("result.raw_arg"),
		Raw("#1 + all_types.integer + #2 + #1 + #3 + #4", RawArgs{"#1": 11, "#2": 22, "#3": 33, "#4": 44}).
			AS("result.raw_arg2"),

		AllTypes.SmallIntPtr.NOT_IN(Int(11), Int(22), NULL).AS("result.not_in"),
		AllTypes.SmallIntPtr.NOT_IN(AllTypes.SELECT(AllTypes.Integer)).AS("result.not_in_select"),
	).FROM(
		AllTypes,
	).LIMIT(2)

	testutils.AssertStatementSql(t, query, strings.Replace(`
SELECT all_types.integer IS NULL AS "result.is_null",
     all_types.date_ptr IS NOT NULL AS "result.is_not_null",
     (all_types.small_int_ptr IN (?, ?)) AS "result.in",
     (all_types.small_int_ptr IN ((
          SELECT all_types.integer AS "all_types.integer"
          FROM all_types
     ))) AS "result.in_select",
     (length(121232459)) AS "result.raw",
     (? + COALESCE(all_types.small_int_ptr, 0) + ?) AS "result.raw_arg",
     (? + all_types.integer + ? + ? + ? + ?) AS "result.raw_arg2",
     (all_types.small_int_ptr NOT IN (?, ?, NULL)) AS "result.not_in",
     (all_types.small_int_ptr NOT IN ((
          SELECT all_types.integer AS "all_types.integer"
          FROM all_types
     ))) AS "result.not_in_select"
FROM all_types
LIMIT ?;
`, "'", "`", -1), int64(11), int64(22), 78, 56, 11, 22, 11, 33, 44, int64(11), int64(22), int64(2))

	var dest []struct {
		common.ExpressionTestResult `alias:"result.*"`
	}

	err := query.Query(sampleDB, &dest)
	require.NoError(t, err)

	require.Equal(t, *dest[0].IsNull, false)
	require.Equal(t, *dest[0].IsNotNull, true)
	require.Equal(t, *dest[0].In, false)
	require.Equal(t, *dest[0].InSelect, false)
	require.Equal(t, *dest[0].Raw, "9")
	require.Equal(t, *dest[0].RawArg, int32(148))
	require.Equal(t, *dest[0].RawArg2, int32(-1479))
	require.Nil(t, dest[0].NotIn)
	require.Equal(t, *dest[0].NotInSelect, true)
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
		AllTypes.Boolean.IS_NULL().AS("is unknown"),
		AllTypes.Boolean.IS_NOT_NULL().AS("is_not_unknown"),

		AllTypes.Boolean.AND(AllTypes.Boolean).EQ(AllTypes.Boolean.AND(AllTypes.Boolean)).AS("complex1"),
		AllTypes.Boolean.OR(AllTypes.Boolean).EQ(AllTypes.Boolean.AND(AllTypes.Boolean)).AS("complex2"),
	)

	testutils.AssertStatementSql(t, query, `
SELECT (all_types.boolean = all_types.boolean_ptr) AS "EQ1",
     (all_types.boolean = ?) AS "EQ2",
     (all_types.boolean != all_types.boolean_ptr) AS "NEq1",
     (all_types.boolean != ?) AS "NEq2",
     (all_types.boolean IS NOT all_types.boolean_ptr) AS "distinct1",
     (all_types.boolean IS NOT ?) AS "distinct2",
     (all_types.boolean IS all_types.boolean_ptr) AS "not_distinct_1",
     (all_types.boolean IS ?) AS "NOTDISTINCT2",
     all_types.boolean IS TRUE AS "ISTRUE",
     all_types.boolean IS NOT TRUE AS "isnottrue",
     all_types.boolean IS FALSE AS "is_False",
     all_types.boolean IS NOT FALSE AS "is not false",
     all_types.boolean IS NULL AS "is unknown",
     all_types.boolean IS NOT NULL AS "is_not_unknown",
     ((all_types.boolean AND all_types.boolean) = (all_types.boolean AND all_types.boolean)) AS "complex1",
     ((all_types.boolean OR all_types.boolean) = (all_types.boolean AND all_types.boolean)) AS "complex2"
FROM all_types;
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

	err := query.Query(sampleDB, &dest)

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
		AllTypes.Numeric.BETWEEN(Float(1.34), AllTypes.Decimal).AS("between"),
		AllTypes.Numeric.NOT_BETWEEN(AllTypes.Decimal.MUL(Float(3)), Float(100.12)).AS("not_between"),

		AllTypes.Decimal.ADD(AllTypes.Decimal).AS("add1"),
		AllTypes.Decimal.ADD(Float(11.22)).AS("add2"),
		AllTypes.Decimal.SUB(AllTypes.DecimalPtr).AS("sub1"),
		AllTypes.Decimal.SUB(Float(11.22)).AS("sub2"),
		AllTypes.Decimal.MUL(AllTypes.DecimalPtr).AS("mul1"),
		AllTypes.Decimal.MUL(Float(11.22)).AS("mul2"),
		AllTypes.Decimal.DIV(AllTypes.DecimalPtr).AS("div1"),
		AllTypes.Decimal.DIV(Float(11.22)).AS("div2"),
		AllTypes.Decimal.MOD(AllTypes.DecimalPtr).AS("mod1"),
		AllTypes.Decimal.MOD(Float(11.22)).AS("mod2"),

		// sqlite driver has to enable SQLITE_ENABLE_MATH_FUNCTIONS before commented math functions can be used

		//AllTypes.Decimal.POW(AllTypes.DecimalPtr).AS("pow1"),
		//AllTypes.Decimal.POW(Float(2.1)).AS("pow2"),

		ABSf(AllTypes.Decimal).AS("abs"),
		//POWER(AllTypes.Decimal, Float(2.1)).AS("power"),
		//SQRT(AllTypes.Decimal).AS("sqrt"),
		//CBRT(AllTypes.Decimal).AS("cbrt"),

		//CEIL(AllTypes.Real).AS("ceil"),
		//FLOOR(AllTypes.Real).AS("floor"),
		ROUND(AllTypes.Decimal).AS("round1"),
		ROUND(AllTypes.Decimal, Int(2)).AS("round2"),
		//TRUNC(AllTypes.Decimal, Int(1)).AS("trunc"),
		SIGN(AllTypes.Real).AS("sign"),
	).LIMIT(1)

	testutils.AssertStatementSql(t, query, `
SELECT (all_types.numeric = all_types.numeric) AS "eq1",
     (all_types.decimal = ?) AS "eq2",
     (all_types.real = ?) AS "eq3",
     (all_types.numeric IS NOT all_types.numeric) AS "distinct1",
     (all_types.decimal IS NOT ?) AS "distinct2",
     (all_types.real IS NOT ?) AS "distinct3",
     (all_types.numeric IS all_types.numeric) AS "not_distinct1",
     (all_types.decimal IS ?) AS "not_distinct2",
     (all_types.real IS ?) AS "not_distinct3",
     (all_types.numeric < ?) AS "lt1",
     (all_types.numeric < ?) AS "lt2",
     (all_types.numeric > ?) AS "gt1",
     (all_types.numeric > ?) AS "gt2",
     (all_types.numeric BETWEEN ? AND all_types.decimal) AS "between",
     (all_types.numeric NOT BETWEEN (all_types.decimal * ?) AND ?) AS "not_between",
     (all_types.decimal + all_types.decimal) AS "add1",
     (all_types.decimal + ?) AS "add2",
     (all_types.decimal - all_types.decimal_ptr) AS "sub1",
     (all_types.decimal - ?) AS "sub2",
     (all_types.decimal * all_types.decimal_ptr) AS "mul1",
     (all_types.decimal * ?) AS "mul2",
     (all_types.decimal / all_types.decimal_ptr) AS "div1",
     (all_types.decimal / ?) AS "div2",
     (all_types.decimal % all_types.decimal_ptr) AS "mod1",
     (all_types.decimal % ?) AS "mod2",
     ABS(all_types.decimal) AS "abs",
     ROUND(all_types.decimal) AS "round1",
     ROUND(all_types.decimal, ?) AS "round2",
     SIGN(all_types.real) AS "sign"
FROM all_types
LIMIT ?;
`)

	var dest struct {
		common.FloatExpressionTestResult `alias:"."`
	}

	err := query.Query(sampleDB, &dest)

	require.NoError(t, err)
	require.Equal(t, *dest.Eq1, true)
	require.Equal(t, *dest.Distinct1, false)
	require.Equal(t, *dest.Lt1, true)
	require.Equal(t, *dest.Add1, 2.22)
	require.Equal(t, *dest.Mod2, float64(1))
	require.Equal(t, *dest.Round1, float64(1))
	require.Equal(t, *dest.Round2, float64(1.11))
	require.Equal(t, *dest.Sign, float64(1))

	//testutils.AssertJSONFile(t, dest, "./testdata/results/common/float_operators.json")
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
		AllTypes.Integer.BETWEEN(Int(11), Int(200)).AS("between"),
		AllTypes.Integer.NOT_BETWEEN(Int(66), Int(77)).AS("not_between"),

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

		//AllTypes.SmallInt.POW(AllTypes.SmallInt.DIV(Int(3))).AS("pow1"),
		//AllTypes.SmallInt.POW(Int(6)).AS("pow2"),

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
		//SQRT(ABSi(AllTypes.BigInt)).AS("sqrt"),
		//CBRT(ABSi(AllTypes.BigInt)).AS("cbrt"),
	).LIMIT(2)

	var dest []struct {
		common.AllTypesIntegerExpResult `alias:"."`
	}

	err := query.Query(sampleDB, &dest)

	require.NoError(t, err)

	require.Equal(t, *dest[0].Eq1, true)
	require.Equal(t, *dest[0].Distinct2, true)
	require.Equal(t, *dest[0].Lt2, false)
	require.Equal(t, *dest[0].Add1, int64(10000))
	require.Equal(t, *dest[0].Mul1, int64(25000000))
	require.Equal(t, *dest[0].Div2, int64(454))
	require.Equal(t, *dest[0].BitAnd1, int64(14))
	require.Equal(t, *dest[0].BitXor2, int64(5))
	require.Equal(t, *dest[0].BitShiftLeft1, int64(1792))
	require.Equal(t, *dest[0].BitShiftRight2, int64(7))
	require.Equal(t, *dest[0].Between, false)
	require.Equal(t, *dest[0].NotBetween, true)
}

func TestStringOperators(t *testing.T) {

	query := SELECT(
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
		AllTypes.Text.BETWEEN(String("min"), String("max")),
		AllTypes.Text.NOT_BETWEEN(AllTypes.VarChar, AllTypes.CharPtr),
		AllTypes.Text.CONCAT(String("text2")),
		AllTypes.Text.CONCAT(Int(11)),
		AllTypes.Text.LIKE(String("abc")),
		AllTypes.Text.NOT_LIKE(String("_b_")),
		//AllTypes.Text.REGEXP_LIKE(String("aba")),
		//AllTypes.Text.REGEXP_LIKE(String("aba"), false),
		//String("ABA").REGEXP_LIKE(String("aba"), true),
		//AllTypes.Text.NOT_REGEXP_LIKE(String("aba")),
		//AllTypes.Text.NOT_REGEXP_LIKE(String("aba"), false),
		//String("ABA").NOT_REGEXP_LIKE(String("aba"), true),

		//BIT_LENGTH(AllTypes.Text),
		//CHAR_LENGTH(AllTypes.Char),
		//OCTET_LENGTH(AllTypes.Text),
		LOWER(AllTypes.VarCharPtr),
		UPPER(AllTypes.Char),
		LTRIM(AllTypes.VarCharPtr),
		RTRIM(AllTypes.VarCharPtr),
		//CONCAT(String("string1"), Int(1), Float(11.12)),
		//CONCAT_WS(String("string1"), Int(1), Float(11.12)),
		//FORMAT(String("Hello %s, %1$s"), String("World")),
		//LEFTSTR(String("abcde"), Int(2)),
		//RIGHTSTR(String("abcde"), Int(2)),
		LENGTH(String("jose")),
		//LPAD(String("Hi"), Int(5), String("xy")),
		//RPAD(String("Hi"), Int(5), String("xy")),
		//MD5(AllTypes.VarCharPtr),
		//REPEAT(AllTypes.Text, Int(33)),
		REPLACE(AllTypes.Char, String("BA"), String("AB")),
		//REVERSE(AllTypes.VarCharPtr),
		SUBSTR(AllTypes.CharPtr, Int(3)),
		SUBSTR(AllTypes.CharPtr, Int(3), Int(2)),
	).FROM(AllTypes)

	var dest []struct{}
	err := query.Query(sampleDB, &dest)

	require.NoError(t, err)
}

func TestBlob(t *testing.T) {

	var sampleBlob = Blob([]byte{11, 0, 22, 33, 44})
	var textBlob = Blob([]byte("text blob"))

	stmt := SELECT(
		AllTypes.Blob.EQ(sampleBlob),
		AllTypes.Blob.EQ(AllTypes.BlobPtr),
		AllTypes.Blob.NOT_EQ(sampleBlob),
		AllTypes.Blob.GT(textBlob),
		AllTypes.Blob.GT_EQ(AllTypes.BlobPtr),
		AllTypes.Blob.LT(AllTypes.BlobPtr),
		AllTypes.Blob.LT_EQ(sampleBlob),
		AllTypes.Blob.BETWEEN(Blob([]byte("min")), Blob([]byte("max"))),
		AllTypes.Blob.NOT_BETWEEN(AllTypes.Blob, AllTypes.BlobPtr),
		AllTypes.Blob.CONCAT(textBlob),
		AllTypes.Blob.LIKE(AllTypes.BlobPtr),
		AllTypes.Blob.NOT_LIKE(sampleBlob),

		RTRIM(AllTypes.BlobPtr, sampleBlob),
		LTRIM(sampleBlob, textBlob),
		LENGTH(sampleBlob),
		OCTET_LENGTH(textBlob),
		SUBSTR(AllTypes.Blob, Int(0), Int(2)),

		HEX(AllTypes.Blob),
		UNHEX(AllTypes.Text),
	).FROM(
		AllTypes,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT all_types.blob = X'0b0016212c',
     all_types.blob = all_types.blob_ptr,
     all_types.blob != X'0b0016212c',
     all_types.blob > X'7465787420626c6f62',
     all_types.blob >= all_types.blob_ptr,
     all_types.blob < all_types.blob_ptr,
     all_types.blob <= X'0b0016212c',
     all_types.blob BETWEEN X'6d696e' AND X'6d6178',
     all_types.blob NOT BETWEEN all_types.blob AND all_types.blob_ptr,
     all_types.blob || X'7465787420626c6f62',
     all_types.blob LIKE all_types.blob_ptr,
     all_types.blob NOT LIKE X'0b0016212c',
     RTRIM(all_types.blob_ptr, X'0b0016212c'),
     LTRIM(X'0b0016212c', X'7465787420626c6f62'),
     LENGTH(X'0b0016212c'),
     OCTET_LENGTH(X'7465787420626c6f62'),
     SUBSTR(all_types.blob, 0, 2),
     HEX(all_types.blob),
     UNHEX(all_types.text)
FROM all_types;
`)

	var dest []struct{}
	err := stmt.Query(sampleDB, &dest)

	require.NoError(t, err)
}

func TestBlobConversion(t *testing.T) {

	nonPrintable := []byte{0x11, 0x22, 0x33, 0x44, 0x55}
	printable := []byte("this is blob")

	stmt := SELECT(
		Blob(nonPrintable).AS("non_printable"),
		Blob(printable).AS("printable"),

		HEX(Blob(nonPrintable)).AS("non_printable_hex"),
		UNHEX(String("1122334455")).AS("non_printable_unhex"),
	)

	var dest struct {
		NonPrintable []byte
		Printable    []byte

		NonPrintableHex   string
		NonPrintableUnHex []byte
	}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, dest.NonPrintable, nonPrintable)
	require.Equal(t, dest.Printable, printable)
	require.Equal(t, dest.NonPrintableHex, hex.EncodeToString(nonPrintable))
	require.Equal(t, dest.NonPrintableUnHex, nonPrintable)
}

func TestReservedWord(t *testing.T) {
	stmt := SELECT(ReservedWords.AllColumns).
		FROM(ReservedWords)

	testutils.AssertDebugStatementSql(t, stmt, strings.Replace(`
SELECT ''ReservedWords''.''column'' AS "ReservedWords.column",
     ''ReservedWords''.use AS "ReservedWords.use",
     ''ReservedWords''.ceil AS "ReservedWords.ceil",
     ''ReservedWords''.''commit'' AS "ReservedWords.commit",
     ''ReservedWords''.''create'' AS "ReservedWords.create",
     ''ReservedWords''.''default'' AS "ReservedWords.default",
     ''ReservedWords''.''desc'' AS "ReservedWords.desc",
     ''ReservedWords''.empty AS "ReservedWords.empty",
     ''ReservedWords''.float AS "ReservedWords.float",
     ''ReservedWords''.''join'' AS "ReservedWords.join",
     ''ReservedWords''.''like'' AS "ReservedWords.like",
     ''ReservedWords''.max AS "ReservedWords.max",
     ''ReservedWords''.rank AS "ReservedWords.rank"
FROM ''ReservedWords'';
`, "''", "`", -1))

	var dest model.ReservedWords
	err := stmt.Query(sampleDB, &dest)
	require.NoError(t, err)
	require.Equal(t, dest, model.ReservedWords{
		Column:  "Column",
		Use:     "CHECK",
		Ceil:    "CEIL",
		Commit:  "COMMIT",
		Create:  "CREATE",
		Default: "DEFAULT",
		Desc:    "DESC",
		Empty:   "EMPTY",
		Float:   "FLOAT",
		Join:    "JOIN",
		Like:    "LIKE",
		Max:     "MAX",
		Rank:    "RANK",
	})
}

func TestExactDecimals(t *testing.T) {

	type exactDecimals struct {
		model.ExactDecimals
		Decimal    decimal.Decimal
		DecimalPtr decimal.Decimal
	}

	t.Run("should query decimal", func(t *testing.T) {
		query := SELECT(
			ExactDecimals.AllColumns,
		).FROM(
			ExactDecimals,
		).WHERE(ExactDecimals.Decimal.EQ(String("1.11111111111111111111")))

		var result exactDecimals

		err := query.Query(sampleDB, &result)
		require.NoError(t, err)

		require.Equal(t, "1.11111111111111111111", result.Decimal.String())
		require.Equal(t, "0", result.DecimalPtr.String()) // NULL

		require.Equal(t, "1.11111111111111111111", result.ExactDecimals.Decimal) // precision loss
		require.Equal(t, (*string)(nil), result.ExactDecimals.DecimalPtr)
		require.Equal(t, "2.22222222222222222222", result.ExactDecimals.Numeric)
		require.Equal(t, (*string)(nil), result.ExactDecimals.NumericPtr) // NULL
	})

	t.Run("should insert decimal", func(t *testing.T) {

		insertQuery := ExactDecimals.INSERT(
			ExactDecimals.AllColumns,
		).MODEL(
			exactDecimals{
				ExactDecimals: model.ExactDecimals{
					// overwritten by wrapped(exactDecimals) scope
					Decimal:    "0.1",
					DecimalPtr: nil,

					// not overwritten
					Numeric:    "6.7",
					NumericPtr: ptr.Of("7.7"),
				},
				Decimal:    decimal.RequireFromString("91.23"),
				DecimalPtr: decimal.RequireFromString("45.67"),
			},
		).RETURNING(ExactDecimals.AllColumns)

		testutils.AssertDebugStatementSql(t, insertQuery, strings.Replace(`
INSERT INTO exact_decimals (decimal, decimal_ptr, numeric, numeric_ptr)
VALUES ('91.23', '45.67', '6.7', '7.7')
RETURNING exact_decimals.decimal AS "exact_decimals.decimal",
          exact_decimals.decimal_ptr AS "exact_decimals.decimal_ptr",
          exact_decimals.numeric AS "exact_decimals.numeric",
          exact_decimals.numeric_ptr AS "exact_decimals.numeric_ptr";
`, "''", "`", -1))

		tx := beginSampleDBTx(t)
		defer tx.Rollback()

		var result exactDecimals

		err := insertQuery.Query(tx, &result)
		require.NoError(t, err)

		require.Equal(t, "91.23", result.Decimal.String())
		require.Equal(t, "45.67", result.DecimalPtr.String())

		require.Equal(t, "6.7", result.ExactDecimals.Numeric)
		require.Equal(t, "7.7", *result.ExactDecimals.NumericPtr)
		require.Equal(t, "91.23", result.ExactDecimals.Decimal)
		require.Equal(t, "45.67", *result.ExactDecimals.DecimalPtr)
	})
}

var timeT = time.Date(2009, 11, 17, 20, 34, 58, 651387237, time.UTC)

func TestDateExpressions(t *testing.T) {

	query := AllTypes.SELECT(
		//Date(2009, 11, 17, 2, MONTH, 1, DAY),

		//DateT(timeT, START_OF_THE_MONTH),
		AllTypes.Date.AS("date"),
		DATE("2009-11-17").AS("date1"),
		DATE("2013-10-07 08:23:19.120", DAYS(1)).AS("date2"),
		DATE(AllTypes.Date, START_OF_YEAR, DAYS(2)).AS("date3"),
		DATE(timeT, START_OF_MONTH).AS("date3"),
		DATE("now", WEEKDAY(1)).AS("date4"),
		DATE(timeT.Unix(), UNIXEPOCH).AS("date5"),
		DATE(time.Now(), UTC).AS("date6"),
		DATE(time.Now().UTC(), LOCALTIME).AS("date7"),

		AllTypes.Date.EQ(AllTypes.Date),
		AllTypes.Date.EQ(Date(2019, 6, 6)),
		AllTypes.DatePtr.NOT_EQ(AllTypes.Date),
		AllTypes.DatePtr.NOT_EQ(Date(2019, 1, 6)),
		AllTypes.Date.IS_DISTINCT_FROM(AllTypes.Date).AS("distinct1"),
		AllTypes.Date.IS_DISTINCT_FROM(Date(2008, 7, 4)).AS("distinct2"),
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
		AllTypes.Date.BETWEEN(Date(2000, 2, 2), AllTypes.DatePtr),
		AllTypes.Date.NOT_BETWEEN(AllTypes.DatePtr, Date(2000, 2, 2)),

		//AllTypes.Date.ADD(INTERVAL2(2, HOUR)),
		//AllTypes.Date.ADD(INTERVAL2(1, DAY, 7, MONTH)),
		//AllTypes.Date.ADD(INTERVALd(25 * time.Hour + 100 * time.Millisecond)),
		//AllTypes.Date.ADD(INTERVALd(-25 * time.Hour - 100 * time.Millisecond)),
		//
		//AllTypes.Date.SUB(INTERVAL(20, MINUTE)),
		//AllTypes.Date.SUB(INTERVALe(AllTypes.SmallInt, MINUTE)),
		//AllTypes.Date.SUB(INTERVALd(3*time.Minute)),

		CURRENT_DATE().AS("current_date"),
	)

	var dest struct {
		Date        string
		Date1       time.Time
		Date2       string
		Date3       time.Time
		Date4       string
		Date5       time.Time
		Date6       string
		Date7       time.Time
		Distinct1   bool
		Distinct2   bool
		CurrentDate time.Time
	}
	err := query.Query(sampleDB, &dest)
	require.NoError(t, err)

	require.Equal(t, dest.Date, "2008-07-04T00:00:00Z")
	require.Equal(t, dest.Date1.Unix(), int64(1258416000))
}

func TestTimeExpressions(t *testing.T) {

	query := AllTypes.SELECT(
		TIME(AllTypes.Time).AS("time1"),
		TIME(timeT).AS("time2"),
		TIME("04:23:19.120-04:00", HOURS(1), MINUTES(2), SECONDS(1.234)).AS("time3"),
		TIME(timeT.Unix(), UNIXEPOCH).AS("time4"),
		TIME(time.Now(), UTC).AS("time5"),
		TIME(time.Now().UTC(), LOCALTIME).AS("time6"),

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
		AllTypes.Time.BETWEEN(Time(11, 0, 30, 100), AllTypes.TimePtr),
		AllTypes.Time.NOT_BETWEEN(AllTypes.TimePtr, TIME(time.Now())),

		AllTypes.Time.LT(AllTypes.Time),
		AllTypes.Time.LT(Time(17, 46, 6)),

		AllTypes.Time.LT_EQ(AllTypes.Time),
		AllTypes.Time.LT_EQ(Time(16, 56, 56)),

		AllTypes.Time.GT(AllTypes.Time),
		AllTypes.Time.GT(Time(15, 16, 46)),

		AllTypes.Time.GT_EQ(AllTypes.Time),
		AllTypes.Time.GT_EQ(Time(14, 26, 36)),

		//AllTypes.Time.ADD(INTERVAL(10, MINUTE)),
		//AllTypes.Time.ADD(INTERVALe(AllTypes.Integer, MINUTE)),
		//AllTypes.Time.ADD(INTERVALd(3*time.Hour)),
		//
		//AllTypes.Time.SUB(INTERVAL(20, MINUTE)),
		//AllTypes.Time.SUB(INTERVALe(AllTypes.SmallInt, MINUTE)),
		//AllTypes.Time.SUB(INTERVALd(3*time.Minute)),
		//
		//AllTypes.Time.ADD(INTERVAL(20, MINUTE)).SUB(INTERVAL(11, HOUR)),

		CURRENT_TIME(),
	)

	var dest struct {
		Time1 string
		Time2 time.Time
		Time3 string
		Time4 time.Time
		Time5 string
		Time6 time.Time
	}
	err := query.Query(sampleDB, &dest)
	require.NoError(t, err)

	require.Equal(t, dest.Time1, "10:11:12")
	require.Equal(t, dest.Time2.UTC().String(), "0000-01-01 20:34:58 +0000 UTC")
	require.Equal(t, dest.Time3, "09:25:20")
}

func TestDateTimeExpressions(t *testing.T) {

	var dateTime = DateTime(2019, 6, 6, 10, 2, 46)

	query := SELECT(
		DATETIME("now").AS("now"),
		DATETIME("2013-10-07T08:23:19.120Z", YEARS(2), MONTHS(1), DAYS(1)).AS("datetime1"),
		DATETIME(AllTypes.DateTime, MONTHS(1), DAYS(1)).AS("datetime2"),
		DATETIME(timeT.Unix(), UNIXEPOCH).AS("datetime3"),
		DATETIME(time.Now(), UTC).AS("datetime4"),
		DATETIME(timeT.UTC(), LOCALTIME).AS("datetime5"),

		JULIANDAY(timeT, DAYS(1)).AS("JulianDay"),
		STRFTIME(String("%H:%M"), timeT, SECONDS(1.22)).AS("strftime"),

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
		AllTypes.DateTime.BETWEEN(AllTypes.DateTimePtr, AllTypes.TimestampPtr),
		AllTypes.DateTime.NOT_BETWEEN(AllTypes.DateTimePtr, AllTypes.TimestampPtr),

		//AllTypes.DateTime.ADD(INTERVAL("05:10:20.000100", HOUR_MICROSECOND)),
		//AllTypes.DateTime.ADD(INTERVALe(AllTypes.BigInt, HOUR)),
		//AllTypes.DateTime.ADD(INTERVALd(2*time.Hour)),
		//
		//AllTypes.DateTime.SUB(INTERVAL("05:10:20.000100", HOUR_MICROSECOND)),
		//AllTypes.DateTime.SUB(INTERVALe(AllTypes.IntegerPtr, HOUR)),
		//AllTypes.DateTime.SUB(INTERVALd(3*time.Hour)),

		CURRENT_TIMESTAMP(),
	).FROM(AllTypes)

	var dest struct {
		Now       time.Time
		DateTime1 time.Time
		DateTime2 time.Time
		DateTime3 time.Time
		DateTime4 time.Time
		DateTime5 time.Time
		JulianDay float64
		StrfTime  string
	}

	err := query.Query(sampleDB, &dest)
	require.NoError(t, err)
	require.True(t, dest.Now.After(time.Now().Add(-1*time.Minute)))
	require.Equal(t, dest.DateTime1.String(), "2015-11-08 08:23:19 +0000 UTC")
	require.Equal(t, dest.DateTime2.String(), "2012-01-19 13:17:17 +0000 UTC")
	require.Equal(t, dest.DateTime3.String(), "2009-11-17 20:34:58 +0000 UTC")
	require.True(t, dest.DateTime4.After(time.Now().Add(-1*time.Minute)))
	require.Equal(t, dest.JulianDay, 2.4551543576232754e+06)
	require.Equal(t, dest.StrfTime, "20:34")
}

func TestRowExpression(t *testing.T) {
	date := Date(2000, 9, 9)
	time := Time(11, 22, 11)
	dateTime := DateTime(2008, 11, 22, 10, 12, 40)
	dateTime2 := DateTime(2011, 1, 2, 5, 12, 40)

	stmt := SELECT(
		ROW(Bool(false), date).EQ(ROW(Bool(true), date)),
		ROW(Bool(false), time).NOT_EQ(ROW(Bool(true), time)),
		ROW(time).IS_DISTINCT_FROM(RowExp(Raw("(time('now'))"))),
		ROW(dateTime, dateTime2).GT(ROW(dateTime, dateTime2)),
		ROW(dateTime2).GT_EQ(ROW(dateTime)),
		ROW(dateTime, dateTime2).LT(ROW(dateTime, dateTime2)),
		ROW(dateTime2).LT_EQ(ROW(dateTime2)),
	)

	//fmt.Println(stmt.Sql())
	//fmt.Println(stmt.DebugSql())

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT (FALSE, DATE('2000-09-09')) = (TRUE, DATE('2000-09-09')),
     (FALSE, TIME('11:22:11')) != (TRUE, TIME('11:22:11')),
     (TIME('11:22:11')) IS NOT ((time('now'))),
     (DATETIME('2008-11-22 10:12:40'), DATETIME('2011-01-02 05:12:40')) > (DATETIME('2008-11-22 10:12:40'), DATETIME('2011-01-02 05:12:40')),
     (DATETIME('2011-01-02 05:12:40')) >= (DATETIME('2008-11-22 10:12:40')),
     (DATETIME('2008-11-22 10:12:40'), DATETIME('2011-01-02 05:12:40')) < (DATETIME('2008-11-22 10:12:40'), DATETIME('2011-01-02 05:12:40')),
     (DATETIME('2011-01-02 05:12:40')) <= (DATETIME('2011-01-02 05:12:40'));
`)

	err := stmt.Query(db, &struct{}{})
	require.NoError(t, err)
}
