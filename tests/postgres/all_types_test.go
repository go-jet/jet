package postgres

import (
	. "github.com/go-jet/jet"
	"github.com/go-jet/jet/internal/testutils"
	"github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/tests/.gentestdata/jetdb/test_sample/table"
	"github.com/google/uuid"
	"gotest.tools/assert"
	"testing"
)

func TestAllTypesSelect(t *testing.T) {
	dest := []model.AllTypes{}

	err := AllTypes.SELECT(AllTypes.AllColumns).Query(db, &dest)
	assert.NilError(t, err)

	assert.DeepEqual(t, dest[0], allTypesRow0)
	assert.DeepEqual(t, dest[1], allTypesRow1)
}

func TestAllTypesInsertModel(t *testing.T) {
	query := AllTypes.INSERT(AllTypes.AllColumns).
		MODEL(allTypesRow0).
		MODEL(&allTypesRow1).
		RETURNING(AllTypes.AllColumns)

	dest := []model.AllTypes{}
	err := query.Query(db, &dest)

	assert.NilError(t, err)
	assert.Equal(t, len(dest), 2)
	assert.DeepEqual(t, dest[0], allTypesRow0)
	assert.DeepEqual(t, dest[1], allTypesRow1)
}

func TestAllTypesInsertQuery(t *testing.T) {
	query := AllTypes.INSERT(AllTypes.AllColumns).
		QUERY(
			AllTypes.
				SELECT(AllTypes.AllColumns).
				LIMIT(2),
		).
		RETURNING(AllTypes.AllColumns)

	dest := []model.AllTypes{}
	err := query.Query(db, &dest)

	assert.NilError(t, err)
	assert.Equal(t, len(dest), 2)
	assert.DeepEqual(t, dest[0], allTypesRow0)
	assert.DeepEqual(t, dest[1], allTypesRow1)
}

func TestExpressionOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Integer.IS_NULL(),
		AllTypes.Timestamp.IS_NOT_NULL(),
		AllTypes.SmallintPtr.IN(Int(11), Int(22), NULL),
		AllTypes.SmallintPtr.IN(AllTypes.SELECT(AllTypes.IntegerPtr)),
		AllTypes.SmallintPtr.NOT_IN(Int(11), Int(22), NULL),
		AllTypes.SmallintPtr.NOT_IN(AllTypes.SELECT(AllTypes.IntegerPtr)),

		CAST(String("TRUE")).AS_BOOL(),
		CAST(String("111")).AS_SMALLINT(),
		CAST(String("111")).AS_INTEGER(),
		CAST(String("111")).AS_BIGINT(),
		CAST(String("11.23")).AS_NUMERIC(30, 10),
		CAST(String("11.23")).AS_NUMERIC(30),
		CAST(String("11.23")).AS_REAL(),
		CAST(String("11.23")).AS_DOUBLE(),
		CAST(Int(234)).AS_TEXT(),
		CAST(String("1/8/1999")).AS_DATE(),
		CAST(String("04:05:06.789")).AS_TIME(),
		CAST(String("04:05:06 PST")).AS_TIMEZ(),
		CAST(String("1999-01-08 04:05:06")).AS_TIMESTAMP(),
		CAST(String("January 8 04:05:06 1999 PST")).AS_TIMESTAMPZ(),

		TO_CHAR(AllTypes.Timestamp, String("HH12:MI:SS")),
		TO_CHAR(AllTypes.Integer, String("999")),
		TO_CHAR(AllTypes.DoublePrecision, String("999D9")),
		TO_CHAR(AllTypes.Numeric, String("999D99S")),

		TO_DATE(String("05 Dec 2000"), String("DD Mon YYYY")),
		TO_NUMBER(String("12,454"), String("99G999D9S")),
		TO_TIMESTAMP(String("05 Dec 2000"), String("DD Mon YYYY")),

		COALESCE(AllTypes.IntegerPtr, AllTypes.SmallintPtr, NULL, Int(11)),
		NULLIF(AllTypes.Text, String("(none)")),
		GREATEST(AllTypes.Numeric, AllTypes.NumericPtr),
		LEAST(AllTypes.Numeric, AllTypes.NumericPtr),

		RAW("current_database()"),
	)

	//fmt.Println(query.DebugSql())

	err := query.Query(db, &struct{}{})

	assert.NilError(t, err)
}

func TestStringOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Text.EQ(AllTypes.Character),
		AllTypes.Text.EQ(String("Text")),
		AllTypes.Text.NOT_EQ(AllTypes.CharacterVaryingPtr),
		AllTypes.Text.NOT_EQ(String("Text")),
		AllTypes.Text.GT(AllTypes.Text),
		AllTypes.Text.GT(String("Text")),
		AllTypes.Text.GT_EQ(AllTypes.TextPtr),
		AllTypes.Text.GT_EQ(String("Text")),
		AllTypes.Text.LT(AllTypes.Character),
		AllTypes.Text.LT(String("Text")),
		AllTypes.Text.LT_EQ(AllTypes.CharacterVaryingPtr),
		AllTypes.Text.LT_EQ(String("Text")),
		AllTypes.Text.CONCAT(String("text2")),
		AllTypes.Text.CONCAT(Int(11)),
		AllTypes.Text.LIKE(String("abc")),
		AllTypes.Text.NOT_LIKE(String("_b_")),
		AllTypes.Text.SIMILAR_TO(String("%(b|d)%")),
		AllTypes.Text.NOT_SIMILAR_TO(String("(b|c)%")),

		BIT_LENGTH(AllTypes.Text),
		CHAR_LENGTH(AllTypes.Character),
		OCTET_LENGTH(AllTypes.Text),
		LOWER(AllTypes.CharacterVaryingPtr),
		UPPER(AllTypes.Character),
		BTRIM(AllTypes.CharacterVarying),
		BTRIM(AllTypes.CharacterVarying, String("AA")),
		LTRIM(AllTypes.CharacterVarying),
		LTRIM(AllTypes.CharacterVarying, String("A")),
		RTRIM(AllTypes.CharacterVarying),
		RTRIM(AllTypes.CharacterVarying, String("B")),
		CHR(Int(65)),
		//CONCAT(String("string1"), Int(1), Float(11.12)),
		//CONCAT_WS(String("string1"), Int(1), Float(11.12)),
		CONVERT(String("text_in_utf8"), String("UTF8"), String("LATIN1")),
		CONVERT_FROM(String("text_in_utf8"), String("UTF8")),
		CONVERT_TO(String("text_in_utf8"), String("UTF8")),
		ENCODE(String("123\000\001"), String("base64")),
		DECODE(String("MTIzAAE="), String("base64")),
		//FORMAT(String("Hello %s, %1$s"), String("World")),
		INITCAP(String("hi THOMAS")),
		LEFT(String("abcde"), Int(2)),
		RIGHT(String("abcde"), Int(2)),
		LENGTH(String("jose")),
		LENGTH(String("jose"), String("UTF8")),
		LPAD(String("Hi"), Int(5)),
		LPAD(String("Hi"), Int(5), String("xy")),
		RPAD(String("Hi"), Int(5)),
		RPAD(String("Hi"), Int(5), String("xy")),
		MD5(AllTypes.CharacterVarying),
		REPEAT(AllTypes.Text, Int(33)),
		REPLACE(AllTypes.Character, String("BA"), String("AB")),
		REVERSE(AllTypes.CharacterVarying),
		STRPOS(AllTypes.Text, String("A")),
		SUBSTR(AllTypes.CharacterPtr, Int(3)),
		SUBSTR(AllTypes.CharacterPtr, Int(3), Int(2)),
		TO_HEX(AllTypes.IntegerPtr),
	)

	//_, args, _ := query.Sql()

	//fmt.Println(query.Sql())
	//fmt.Println(args[15])
	//fmt.Println(query.DebugSql())

	err := query.Query(db, &struct{}{})

	assert.NilError(t, err)
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

	assert.NilError(t, err)

	testutils.AssertJSONFile(t, "./testdata/common/bool_operators.json", dest)
}

func TestFloatOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Numeric.EQ(AllTypes.Numeric),
		AllTypes.Decimal.EQ(Float(12)),
		AllTypes.Real.EQ(Float(12.12)),
		AllTypes.Numeric.IS_DISTINCT_FROM(AllTypes.Numeric),
		AllTypes.Decimal.IS_DISTINCT_FROM(Float(12)),
		AllTypes.Real.IS_DISTINCT_FROM(Float(12.12)),
		AllTypes.Numeric.IS_NOT_DISTINCT_FROM(AllTypes.Numeric),
		AllTypes.Decimal.IS_NOT_DISTINCT_FROM(Float(12)),
		AllTypes.Real.IS_NOT_DISTINCT_FROM(Float(12.12)),
		//AllTypes.Numeric.LT(AllTypes.Integer),
		AllTypes.Numeric.LT(Float(124)),
		AllTypes.Numeric.LT(Float(34.56)),
		//AllTypes.Numeric.GT(AllTypes.Smallint),
		AllTypes.Numeric.GT(Float(124)),
		AllTypes.Numeric.GT(Float(34.56)),

		AllTypes.Real.ADD(AllTypes.RealPtr),
		AllTypes.Real.ADD(Float(11.22)),
		AllTypes.Real.SUB(AllTypes.RealPtr),
		AllTypes.Real.SUB(Float(11.22)),
		AllTypes.Real.MUL(AllTypes.RealPtr),
		AllTypes.Real.MUL(Float(11.22)),
		AllTypes.Real.DIV(AllTypes.RealPtr),
		AllTypes.Real.DIV(Float(11.22)),
		AllTypes.Decimal.MOD(AllTypes.Decimal),
		AllTypes.Decimal.MOD(Float(11.22)),
		AllTypes.Real.POW(AllTypes.RealPtr),
		AllTypes.Real.POW(Float(11.22)),

		ABSf(AllTypes.Real),
		SQRT(AllTypes.Real),
		CBRT(AllTypes.Real),
		CEIL(AllTypes.Real),
		FLOOR(AllTypes.Real),
		ROUND(AllTypes.Decimal),
		ROUND(AllTypes.Decimal, AllTypes.Integer).AS("round"),
		SIGN(AllTypes.Real),
		TRUNC(AllTypes.Decimal),
		TRUNC(AllTypes.Decimal, Int(1)),
	)

	//fmt.Println(query.DebugSql())

	err := query.Query(db, &struct{}{})

	assert.NilError(t, err)
}

func TestIntegerOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Integer.EQ(AllTypes.IntegerPtr),
		AllTypes.Bigint.EQ(Int(12)),
		//AllTypes.Smallint.NOT_EQ(AllTypes.Real),
		AllTypes.Integer.NOT_EQ(AllTypes.IntegerPtr),
		AllTypes.Bigint.NOT_EQ(Int(12)),
		AllTypes.Integer.LT(AllTypes.IntegerPtr),
		AllTypes.Bigint.LT(Int(65)),
		//AllTypes.Smallint.LT_EQ(AllTypes.Numeric),
		AllTypes.Integer.LT_EQ(AllTypes.IntegerPtr),
		AllTypes.Bigint.LT_EQ(Int(65)),
		//AllTypes.Smallint.GT_EQ(AllTypes.Numeric),
		AllTypes.Integer.GT(AllTypes.IntegerPtr),
		AllTypes.Bigint.GT(Int(65)),
		AllTypes.Integer.GT_EQ(AllTypes.IntegerPtr),
		AllTypes.Bigint.GT_EQ(Int(65)),

		AllTypes.Integer.ADD(AllTypes.Integer),
		AllTypes.Integer.ADD(Int(11)),
		AllTypes.Integer.SUB(AllTypes.Integer),
		AllTypes.Integer.SUB(Int(11)),
		AllTypes.Integer.MUL(AllTypes.Integer),
		AllTypes.Integer.MUL(Int(11)),
		AllTypes.Integer.DIV(AllTypes.Integer),
		AllTypes.Integer.DIV(Int(11)),
		AllTypes.Integer.MOD(AllTypes.Integer),
		AllTypes.Integer.MOD(Int(11)),
		AllTypes.Integer.POW(AllTypes.Smallint),
		AllTypes.Integer.POW(Int(11)),
		AllTypes.Integer.BIT_AND(AllTypes.Smallint),
		AllTypes.Integer.BIT_OR(AllTypes.Smallint),
		AllTypes.Integer.BIT_XOR(Int(11)),
		BIT_NOT(AllTypes.Integer),
		AllTypes.Integer.BIT_SHIFT_LEFT(AllTypes.Smallint),
		AllTypes.Integer.BIT_SHIFT_LEFT(Int(11)),
		AllTypes.Integer.BIT_SHIFT_RIGHT(AllTypes.Smallint),
		AllTypes.Integer.BIT_SHIFT_RIGHT(Int(11)),

		ABSi(AllTypes.Integer),
		SQRT(AllTypes.Integer),
		CBRT(AllTypes.Integer),
	)

	//fmt.Println(query.DebugSql())

	err := query.Query(db, &struct{}{})

	assert.NilError(t, err)
}

func TestTimeOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Time.EQ(AllTypes.Time),
		AllTypes.Time.EQ(Time(23, 6, 6, 1)),
		AllTypes.Timez.EQ(AllTypes.TimezPtr),
		AllTypes.Timez.EQ(Timez(23, 6, 6, 222, +200)),
		AllTypes.Timestamp.EQ(AllTypes.TimestampPtr),
		AllTypes.Timestamp.EQ(Timestamp(2010, 10, 21, 15, 30, 12, 333)),
		AllTypes.Timestampz.EQ(AllTypes.TimestampzPtr),
		AllTypes.Timestampz.EQ(Timestampz(2010, 10, 21, 15, 30, 12, 444, 0)),
		AllTypes.Date.EQ(AllTypes.DatePtr),
		AllTypes.Date.EQ(Date(2010, 12, 3)),

		AllTypes.Time.NOT_EQ(AllTypes.Time),
		AllTypes.Time.NOT_EQ(Time(23, 6, 6, 10)),
		AllTypes.Timez.NOT_EQ(AllTypes.TimezPtr),
		AllTypes.Timez.NOT_EQ(Timez(23, 6, 6, 555, +200)),
		AllTypes.Timestamp.NOT_EQ(AllTypes.TimestampPtr),
		AllTypes.Timestamp.NOT_EQ(Timestamp(2010, 10, 21, 15, 30, 12, 666)),
		AllTypes.Timestampz.NOT_EQ(AllTypes.TimestampzPtr),
		AllTypes.Timestampz.NOT_EQ(Timestampz(2010, 10, 21, 15, 30, 12, 777, 0)),
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

	err := query.Query(db, &struct{}{})

	assert.NilError(t, err)
}

func TestSubQueryColumnReference(t *testing.T) {

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

	unionexpectedSQL := `
     (
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
		assert.NilError(t, err)
		assert.Equal(t, len(dest1), 2)
		assert.Equal(t, dest1[0].Boolean, allTypesRow0.Boolean)
		assert.Equal(t, dest1[0].Integer, allTypesRow0.Integer)
		assert.Equal(t, dest1[0].Real, allTypesRow0.Real)
		assert.Equal(t, dest1[0].Text, allTypesRow0.Text)
		assert.DeepEqual(t, dest1[0].Time, allTypesRow0.Time)
		assert.DeepEqual(t, dest1[0].Timez, allTypesRow0.Timez)
		assert.DeepEqual(t, dest1[0].Timestamp, allTypesRow0.Timestamp)
		assert.DeepEqual(t, dest1[0].Timestampz, allTypesRow0.Timestampz)
		assert.DeepEqual(t, dest1[0].Date, allTypesRow0.Date)

		stmt2 := SELECT(
			subQuery.AllColumns(),
		).
			FROM(subQuery)

		//fmt.Println(stmt2.DebugSql())

		testutils.AssertDebugStatementSql(t, stmt2, expectedSQL+expected.sql+";\n", expected.args...)

		dest2 := []model.AllTypes{}
		err = stmt2.Query(db, &dest2)

		assert.NilError(t, err)
		assert.DeepEqual(t, dest1, dest2)
	}
}

var allTypesRow0 = model.AllTypes{
	SmallintPtr:        Int16Ptr(1),
	Smallint:           1,
	IntegerPtr:         Int32Ptr(300),
	Integer:            300,
	BigintPtr:          Int64Ptr(50000),
	Bigint:             5000,
	DecimalPtr:         Float64Ptr(11.44),
	Decimal:            11.44,
	NumericPtr:         Float64Ptr(55.77),
	Numeric:            55.77,
	RealPtr:            Float32Ptr(99.1),
	Real:               99.1,
	DoublePrecisionPtr: Float64Ptr(11111111.22),
	DoublePrecision:    11111111.22,
	Smallserial:        1,
	Serial:             1,
	Bigserial:          1,
	//MoneyPtr: nil,
	//Money:
	CharacterVaryingPtr:  StringPtr("ABBA"),
	CharacterVarying:     "ABBA",
	CharacterPtr:         StringPtr("JOHN                                                                            "),
	Character:            "JOHN                                                                            ",
	TextPtr:              StringPtr("Some text"),
	Text:                 "Some text",
	ByteaPtr:             ByteArrayPtr([]byte("bytea")),
	Bytea:                []byte("bytea"),
	TimestampzPtr:        TimestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	Timestampz:           *TimestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	TimestampPtr:         testutils.TimestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	Timestamp:            *testutils.TimestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	DatePtr:              testutils.TimestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	Date:                 *testutils.TimestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	TimezPtr:             TimeWithTimeZone("04:05:06 -0800"),
	Timez:                *TimeWithTimeZone("04:05:06 -0800"),
	TimePtr:              TimeWithoutTimeZone("04:05:06"),
	Time:                 *TimeWithoutTimeZone("04:05:06"),
	IntervalPtr:          StringPtr("3 days 04:05:06"),
	Interval:             "3 days 04:05:06",
	BooleanPtr:           BoolPtr(true),
	Boolean:              false,
	PointPtr:             StringPtr("(2,3)"),
	BitPtr:               StringPtr("101"),
	Bit:                  "101",
	BitVaryingPtr:        StringPtr("101111"),
	BitVarying:           "101111",
	TsvectorPtr:          StringPtr("'supernova':1"),
	Tsvector:             "'supernova':1",
	UUIDPtr:              UUIDPtr("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	UUID:                 uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	XMLPtr:               StringPtr("<Sub>abc</Sub>"),
	XML:                  "<Sub>abc</Sub>",
	JSONPtr:              StringPtr(`{"a": 1, "b": 3}`),
	JSON:                 `{"a": 1, "b": 3}`,
	JsonbPtr:             StringPtr(`{"a": 1, "b": 3}`),
	Jsonb:                `{"a": 1, "b": 3}`,
	IntegerArrayPtr:      StringPtr("{1,2,3}"),
	IntegerArray:         "{1,2,3}",
	TextArrayPtr:         StringPtr("{breakfast,consulting}"),
	TextArray:            "{breakfast,consulting}",
	JsonbArray:           `{"{\"a\": 1, \"b\": 2}","{\"a\": 3, \"b\": 4}"}`,
	TextMultiDimArrayPtr: StringPtr("{{meeting,lunch},{training,presentation}}"),
	TextMultiDimArray:    "{{meeting,lunch},{training,presentation}}",
}

var allTypesRow1 = model.AllTypes{
	SmallintPtr:        nil,
	Smallint:           1,
	IntegerPtr:         nil,
	Integer:            300,
	BigintPtr:          nil,
	Bigint:             5000,
	DecimalPtr:         nil,
	Decimal:            11.44,
	NumericPtr:         nil,
	Numeric:            55.77,
	RealPtr:            nil,
	Real:               99.1,
	DoublePrecisionPtr: nil,
	DoublePrecision:    11111111.22,
	Smallserial:        2,
	Serial:             2,
	Bigserial:          2,
	//MoneyPtr: nil,
	//Money:
	CharacterVaryingPtr:  nil,
	CharacterVarying:     "ABBA",
	CharacterPtr:         nil,
	Character:            "JOHN                                                                            ",
	TextPtr:              nil,
	Text:                 "Some text",
	ByteaPtr:             nil,
	Bytea:                []byte("bytea"),
	TimestampzPtr:        nil,
	Timestampz:           *TimestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	TimestampPtr:         nil,
	Timestamp:            *testutils.TimestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	DatePtr:              nil,
	Date:                 *testutils.TimestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	TimezPtr:             nil,
	Timez:                *TimeWithTimeZone("04:05:06 -0800"),
	TimePtr:              nil,
	Time:                 *TimeWithoutTimeZone("04:05:06"),
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
