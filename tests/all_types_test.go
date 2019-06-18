package tests

import (
	"fmt"
	. "github.com/go-jet/jet/sqlbuilder"
	"github.com/go-jet/jet/tests/.test_files/dvd_rental/test_sample/model"
	. "github.com/go-jet/jet/tests/.test_files/dvd_rental/test_sample/table"
	"github.com/google/uuid"
	"gotest.tools/assert"
	"testing"
)

func TestAllTypesSelect(t *testing.T) {

	dest := []model.AllTypes{}

	err := AllTypes.SELECT(AllTypes.AllColumns).Query(db, &dest)

	fmt.Println(err)
	assert.NilError(t, err)

	assert.Equal(t, len(dest), 2)

	assert.DeepEqual(t, dest[0], allTypesRow0)
	assert.DeepEqual(t, dest[1], allTypesRow1)
}

func TestAllTypesInsertModel(t *testing.T) {
	query := AllTypes.INSERT(AllTypes.AllColumns).
		USING(allTypesRow0).
		USING(&allTypesRow1).
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

		String("TRUE").TO_BOOL(),
		String("111").TO_SMALLINT(),
		String("111").TO_INTEGER(),
		String("111").TO_BIGINT(),
		String("11.23").TO_NUMERIC(30, 10),
		String("11.23").TO_NUMERIC(30),
		String("11.23").TO_REAL(),
		String("11.23").TO_DOUBLE(),
		Int(234).TO_TEXT(),
		String("1/8/1999").TO_DATE(),
		String("04:05:06.789").TO_TIME(),
		String("04:05:06 PST").TO_TIMEZ(),
		String("1999-01-08 04:05:06").TO_TIMESTAMP(),
		String("January 8 04:05:06 1999 PST").TO_TIMESTAMPZ(),

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

	fmt.Println(query.DebugSql())

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
		LTRIM(AllTypes.CharacterVarying, String("A")),
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
		LPAD(String("Hi"), Int(5), String("xy")),
		RPAD(String("Hi"), Int(5), String("xy")),
		MD5(AllTypes.CharacterVarying),
		REPEAT(AllTypes.Text, Int(33)),
		REPLACE(AllTypes.Character, String("BA"), String("AB")),
		REVERSE(AllTypes.CharacterVarying),
		STRPOS(AllTypes.Text, String("A")),
		SUBSTR(AllTypes.CharacterPtr, Int(3), Int(2)),
		TO_HEX(AllTypes.IntegerPtr),
	)

	_, args, _ := query.Sql()

	fmt.Println(query.Sql())
	fmt.Println(args[15])
	fmt.Println(query.DebugSql())

	err := query.Query(db, &struct{}{})

	assert.NilError(t, err)
}

func TestBoolOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Boolean.EQ(AllTypes.BooleanPtr),
		AllTypes.Boolean.EQ(Bool(true)),
		AllTypes.Boolean.NOT_EQ(AllTypes.BooleanPtr),
		AllTypes.Boolean.NOT_EQ(Bool(false)),
		AllTypes.Boolean.IS_DISTINCT_FROM(AllTypes.BooleanPtr),
		AllTypes.Boolean.IS_DISTINCT_FROM(Bool(true)),
		AllTypes.Boolean.IS_NOT_DISTINCT_FROM(AllTypes.BooleanPtr),
		AllTypes.Boolean.IS_NOT_DISTINCT_FROM(Bool(true)),
		AllTypes.Boolean.IS_TRUE(),
		AllTypes.Boolean.IS_NOT_TRUE(),
		AllTypes.Boolean.IS_NOT_FALSE(),
		AllTypes.Boolean.IS_UNKNOWN(),
		AllTypes.Boolean.IS_NOT_UNKNOWN(),

		AllTypes.Boolean.AND(AllTypes.Boolean).EQ(AllTypes.Boolean.AND(AllTypes.Boolean)),
		AllTypes.Boolean.OR(AllTypes.Boolean).EQ(AllTypes.Boolean.AND(AllTypes.Boolean)),
	)

	fmt.Println(query.DebugSql())

	err := query.Query(db, &struct{}{})

	assert.NilError(t, err)
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
		SQRTf(AllTypes.Real),
		CBRTf(AllTypes.Real),
		CEIL(AllTypes.Real),
		FLOOR(AllTypes.Real),
		ROUND(AllTypes.Decimal),
		ROUND(AllTypes.Decimal, Int(3)).AS("round"),
		SIGN(AllTypes.Real),
		TRUNC(AllTypes.Decimal),
		TRUNC(AllTypes.Decimal, Int(1)),
	)

	fmt.Println(query.DebugSql())

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
		AllTypes.Integer.BIT_SHIFT_LEFT(AllTypes.Smallint),
		AllTypes.Integer.BIT_SHIFT_LEFT(Int(11)),
		AllTypes.Integer.BIT_SHIFT_RIGHT(AllTypes.Smallint),
		AllTypes.Integer.BIT_SHIFT_RIGHT(Int(11)),

		ABSi(AllTypes.Integer),
		SQRTi(AllTypes.Integer),
		CBRTi(AllTypes.Integer),
	)

	fmt.Println(query.DebugSql())

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

	fmt.Println(query.DebugSql())

	err := query.Query(db, &struct{}{})

	assert.NilError(t, err)
}

func TestSubQueryColumnReference(t *testing.T) {

	type expected struct {
		sql  string
		args []interface{}
	}

	subQueries := map[ExpressionTable]expected{}

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

	var selectExpectedSql = ` (
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

	unionExpectedSql := `
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

	subQueries[selectSubQuery] = expected{sql: selectExpectedSql, args: []interface{}{int64(2)}}
	subQueries[unionSubQuery] = expected{sql: unionExpectedSql, args: []interface{}{int64(1), int64(1), int64(1)}}

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

		var expectedSql = `
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

		assertStatementSql(t, stmt1, expectedSql+expected.sql+";\n", expected.args...)

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

		fmt.Println(stmt2.DebugSql())

		assertStatementSql(t, stmt2, expectedSql+expected.sql+";\n", expected.args...)

		dest2 := []model.AllTypes{}
		err = stmt2.Query(db, &dest2)

		assert.NilError(t, err)
		assert.DeepEqual(t, dest1, dest2)
	}
}

var allTypesRow0 = model.AllTypes{
	SmallintPtr:        int16Ptr(1),
	Smallint:           1,
	IntegerPtr:         int32Ptr(300),
	Integer:            300,
	BigintPtr:          int64Ptr(50000),
	Bigint:             5000,
	DecimalPtr:         float64Ptr(11.44),
	Decimal:            11.44,
	NumericPtr:         float64Ptr(55.77),
	Numeric:            55.77,
	RealPtr:            float32Ptr(99.1),
	Real:               99.1,
	DoublePrecisionPtr: float64Ptr(11111111.22),
	DoublePrecision:    11111111.22,
	Smallserial:        1,
	Serial:             1,
	Bigserial:          1,
	//MoneyPtr: nil,
	//Money:
	CharacterVaryingPtr:  stringPtr("ABBA"),
	CharacterVarying:     "ABBA",
	CharacterPtr:         stringPtr("JOHN                                                                            "),
	Character:            "JOHN                                                                            ",
	TextPtr:              stringPtr("Some text"),
	Text:                 "Some text",
	ByteaPtr:             byteArrayPtr([]byte("bytea")),
	Bytea:                []byte("bytea"),
	TimestampzPtr:        timestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	Timestampz:           *timestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	TimestampPtr:         timestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	Timestamp:            *timestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	DatePtr:              timestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	Date:                 *timestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	TimezPtr:             timeWithTimeZone("04:05:06 -0800"),
	Timez:                *timeWithTimeZone("04:05:06 -0800"),
	TimePtr:              timeWithoutTimeZone("04:05:06"),
	Time:                 *timeWithoutTimeZone("04:05:06"),
	IntervalPtr:          stringPtr("3 days 04:05:06"),
	Interval:             "3 days 04:05:06",
	BooleanPtr:           boolPtr(true),
	Boolean:              false,
	PointPtr:             stringPtr("(2,3)"),
	BitPtr:               stringPtr("101"),
	Bit:                  "101",
	BitVaryingPtr:        stringPtr("101111"),
	BitVarying:           "101111",
	TsvectorPtr:          stringPtr("'supernova':1"),
	Tsvector:             "'supernova':1",
	UUIDPtr:              uuidPtr("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	UUID:                 uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	XMLPtr:               stringPtr("<Sub>abc</Sub>"),
	XML:                  "<Sub>abc</Sub>",
	JSONPtr:              stringPtr(`{"a": 1, "b": 3}`),
	JSON:                 `{"a": 1, "b": 3}`,
	JsonbPtr:             stringPtr(`{"a": 1, "b": 3}`),
	Jsonb:                `{"a": 1, "b": 3}`,
	IntegerArrayPtr:      stringPtr("{1,2,3}"),
	IntegerArray:         "{1,2,3}",
	TextArrayPtr:         stringPtr("{breakfast,consulting}"),
	TextArray:            "{breakfast,consulting}",
	JsonbArray:           `{"{\"a\": 1, \"b\": 2}","{\"a\": 3, \"b\": 4}"}`,
	TextMultiDimArrayPtr: stringPtr("{{meeting,lunch},{training,presentation}}"),
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
	Timestampz:           *timestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	TimestampPtr:         nil,
	Timestamp:            *timestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	DatePtr:              nil,
	Date:                 *timestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	TimezPtr:             nil,
	Timez:                *timeWithTimeZone("04:05:06 -0800"),
	TimePtr:              nil,
	Time:                 *timeWithoutTimeZone("04:05:06"),
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
