package postgres

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/internal/utils/ptr"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/enum"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestArraySelect(t *testing.T) {
	stmt := SELECT(
		SampleArrays.AllColumns,
	).FROM(
		SampleArrays,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT sample_arrays.id AS "sample_arrays.id",
     sample_arrays.bool_array AS "sample_arrays.bool_array",
     sample_arrays.int2_array_ptr AS "sample_arrays.int2_array_ptr",
     sample_arrays.int4_array AS "sample_arrays.int4_array",
     sample_arrays.int8_array AS "sample_arrays.int8_array",
     sample_arrays.numeric_array AS "sample_arrays.numeric_array",
     sample_arrays.decimal_array AS "sample_arrays.decimal_array",
     sample_arrays.real_array AS "sample_arrays.real_array",
     sample_arrays.double_array AS "sample_arrays.double_array",
     sample_arrays.text_array AS "sample_arrays.text_array",
     sample_arrays.varchar_array AS "sample_arrays.varchar_array",
     sample_arrays.char_array AS "sample_arrays.char_array",
     sample_arrays.bytea_array AS "sample_arrays.bytea_array",
     sample_arrays.date_array AS "sample_arrays.date_array",
     sample_arrays.timestamp_array AS "sample_arrays.timestamp_array",
     sample_arrays.timestamptz_array AS "sample_arrays.timestamptz_array",
     sample_arrays.time_array AS "sample_arrays.time_array",
     sample_arrays.timetz_array AS "sample_arrays.timetz_array",
     sample_arrays.interval_array AS "sample_arrays.interval_array",
     sample_arrays.uuid_array AS "sample_arrays.uuid_array",
     sample_arrays.mood_enum_array AS "sample_arrays.mood_enum_array"
FROM test_sample.sample_arrays;
`)

	var dest []model.SampleArrays

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	require.Len(t, dest, 1)
	require.Equal(t, dest[0], sampleArrayRow0)
}

func TestArrayOperations(t *testing.T) {

	boolArray := BoolArray(true, false, false, true)
	int32Array := Int32Array(-1, 2, -3, 4, 5)
	int64Array := Int64Array(10, -11, 12, -13, 14, 1515551)
	float32Array := Float32Array(1.01, -2.02, 3.03, 4.04)
	float64Array := Float64Array(10.001, 20.002, -3.003, 400050.04)
	stringArray := StringArray("temp", "text", "array")
	byteaArray := ByteaArray([]byte("temporal"), []byte("byte array"), []byte("array"))
	dateArray := DateArray(*testutils.Date("2022-04-06"), *testutils.Date("2018-05-06"))
	dateArray2 := ARRAY(Date(2022, 2, 3), Date(2023, 2, 2))
	timestampArray := TimestampArray(*testutils.TimestampWithoutTimeZone("1999-01-08 04:05:06", 0))
	timestampzArray := TimestampzArray(time.Date(2022, 1, 1, 8, 30, 50, 0, time.UTC))
	timestampzArray2 := ARRAY(Timestampz(2023, 2, 3, 10, 29, 34, 1000, "UTC"))
	timeArray := TimeArray(*testutils.TimeWithoutTimeZone("14:20:45"), *testutils.TimeWithoutTimeZone("8:10:15"))
	timezArray := TimezArray(*testutils.TimeWithTimeZone("04:05:06 -0800"), *testutils.TimeWithTimeZone("01:02:03 -0600"))
	timeArray2 := ARRAY(Time(10, 20, 30), Time(5, 45, 45))
	timezArray2 := ARRAY(Timez(10, 20, 30, 0, "UTC"), Timez(5, 45, 45, 0, "UTC"))

	timestampz := Timestampz(1950, 2, 3, 10, 30, 40, 0, "UTC")

	query := SELECT(
		// array constructors
		boolArray.AS("bool_array"),
		int32Array.AS("int32_array"),
		int64Array.AS("int64_array"),
		float32Array.AS("float32_array"),
		float64Array.AS("float64_array"),
		stringArray.AS("string_array"),
		byteaArray.AS("bytea_array"),
		dateArray.AS("date_array"),
		timestampArray.AS("timestamp_array"),
		timestampzArray.AS("timestampz_array"),
		timeArray.AS("time_array"),
		timezArray.AS("timez_array"),

		// operators
		SampleArrays.BoolArray.EQ(boolArray).AS("bool_eq"),
		SampleArrays.TextArray.EQ(SampleArrays.TextArray).AS("text_eq"),
		SampleArrays.TextArray.NOT_EQ(stringArray).AS("text_neq"),
		SampleArrays.Int4Array.LT(int32Array).IS_TRUE().AS("int4_lt"),
		SampleArrays.Int8Array.LT_EQ(int64Array).IS_FALSE().AS("int8_lteq"),
		SampleArrays.RealArray.GT(float32Array).AS("decimal_gt"),
		SampleArrays.DoubleArray.GT_EQ(float64Array).AS("numeric_gt_eq"),
		SampleArrays.ByteaArray.CONTAINS(byteaArray).AS("bytea_contains"),
		byteaArray.IS_CONTAINED_BY(SampleArrays.ByteaArray).AS("bytea_contained_by"),
		SampleArrays.DateArray.OVERLAP(dateArray2).AS("date_overlaps"),
		SampleArrays.TimestampArray.CONCAT(timestampArray).AS("timestamp_concat"),
		timestampzArray2.CONCAT_ELEMENT(timestampz).AS("timestampz_concat_elem"),
		SampleArrays.TimeArray.AT(Int32(1)).AS("time_at"),

		Int32(22).EQ(ANY(SampleArrays.Int4Array)).AS("int32_eq_any"),
		Double(7.89).NOT_EQ(ANY(SampleArrays.DoubleArray)).AS("double_neq_any"),
		String("temp").EQ(ALL(stringArray)).AS("string_eq_all"),

		// functions
		ARRAY_APPEND(SampleArrays.TextArray, String("after")).AS("append"),
		ARRAY_CAT(SampleArrays.TimeArray, timeArray2).AS("cat"),
		ARRAY_LENGTH(timezArray2, Int32(1)).AS("length"),
		// ARRAY_LOWER(SampleArrays.UUIDArray).AS("lower"),
		ARRAY_POSITION(SampleArrays.Int4Array, Int32(30)).AS("position"),
		//ARRAY_POSITION(SampleArrays.DoubleArray, Float(33.33), Int(1)).AS("position_from"),
		ARRAY_POSITIONS(stringArray, String("text")).AS("positions"),
		ARRAY_PREPEND(String("before"), SampleArrays.TextArray).AS("prepend"),
		ARRAY_REMOVE(boolArray, Bool(true)).AS("remove"),
		ARRAY_REPLACE(SampleArrays.VarcharArray, String("hello"), String("hi")).AS("replace"),
		//ARRAY_REVERSE(SampleArrays.TextArray).AS("reverse"),
		//ARRAY_SAMPLE(SampleArrays.TimestampArray, Int(2)).AS("sample"),
		//ARRAY_SHUFFLE(SampleArrays.BoolArray).AS("shuffle"),
		//ARRAY_SORT(SampleArrays.Int8Array, Bool(true)),
		ARRAY_TO_STRING(SampleArrays.MoodEnumArray, String(", ")).AS("to_string"),
		ARRAY_UPPER(SampleArrays.Int8Array, Int(1)).AS("upper"),
		CARDINALITY(SampleArrays.DoubleArray).AS("cardinality"),

		// unsupported by cockroachdb

		//ARRAY_DIMS(SampleArrays.IntervalArray).AS("dims"),
		//ARRAY_NDIMS(SampleArrays.TextArray).AS("ndims"),
		//TRIM_ARRAY(SampleArrays.DateArray, Int(1)).AS("trim"),

	).FROM(
		SampleArrays,
	).WHERE(
		SampleArrays.BoolArray.CONTAINS(BoolArray(true)),
	)

	testutils.AssertStatementSql(t, query, `
SELECT $1::boolean[] AS "bool_array",
     $2::integer[] AS "int32_array",
     $3::bigint[] AS "int64_array",
     $4::real[] AS "float32_array",
     $5::double precision[] AS "float64_array",
     $6::text[] AS "string_array",
     $7::bytea[] AS "bytea_array",
     $8::date[] AS "date_array",
     $9::timestamp without time zone[] AS "timestamp_array",
     $10::timestamp with time zone[] AS "timestampz_array",
     $11::time without time zone[] AS "time_array",
     $12::time with time zone[] AS "timez_array",
     (sample_arrays.bool_array = $13::boolean[]) AS "bool_eq",
     (sample_arrays.text_array = sample_arrays.text_array) AS "text_eq",
     (sample_arrays.text_array != $14::text[]) AS "text_neq",
     (sample_arrays.int4_array < $15::integer[]) IS TRUE AS "int4_lt",
     (sample_arrays.int8_array <= $16::bigint[]) IS FALSE AS "int8_lteq",
     (sample_arrays.real_array > $17::real[]) AS "decimal_gt",
     (sample_arrays.double_array >= $18::double precision[]) AS "numeric_gt_eq",
     (sample_arrays.bytea_array @> $19::bytea[]) AS "bytea_contains",
     ($20::bytea[] <@ sample_arrays.bytea_array) AS "bytea_contained_by",
     (sample_arrays.date_array && ARRAY[$21::date,$22::date]) AS "date_overlaps",
     (sample_arrays.timestamp_array || $23::timestamp without time zone[]) AS "timestamp_concat",
     (ARRAY[$24::timestamp with time zone] || $25::timestamp with time zone) AS "timestampz_concat_elem",
     sample_arrays.time_array[$26::integer] AS "time_at",
     ($27::integer = ANY(sample_arrays.int4_array)) AS "int32_eq_any",
     ($28::double precision != ANY(sample_arrays.double_array)) AS "double_neq_any",
     ($29::text = ALL($30::text[])) AS "string_eq_all",
     ARRAY_APPEND(sample_arrays.text_array, $31::text) AS "append",
     ARRAY_CAT(sample_arrays.time_array, ARRAY[$32::time without time zone,$33::time without time zone]) AS "cat",
     ARRAY_LENGTH(ARRAY[$34::time with time zone,$35::time with time zone], $36::integer) AS "length",
     ARRAY_POSITION(sample_arrays.int4_array, $37::integer) AS "position",
     ARRAY_POSITIONS($38::text[], $39::text) AS "positions",
     ARRAY_PREPEND($40::text, sample_arrays.text_array) AS "prepend",
     ARRAY_REMOVE($41::boolean[], $42::boolean) AS "remove",
     ARRAY_REPLACE(sample_arrays.varchar_array, $43::text, $44::text) AS "replace",
     ARRAY_TO_STRING(sample_arrays.mood_enum_array, $45::text) AS "to_string",
     ARRAY_UPPER(sample_arrays.int8_array, $46) AS "upper",
     CARDINALITY(sample_arrays.double_array) AS "cardinality"
FROM test_sample.sample_arrays
WHERE sample_arrays.bool_array @> $47::boolean[];
`)

	var dest struct {
		// array constructors
		BoolArray       pq.BoolArray
		Int32Array      pq.Int32Array
		Int64Array      pq.Int64Array
		Float32Array    pq.Float32Array
		Float64Array    pq.Float64Array
		StringArray     pq.StringArray
		ByteaArray      pq.ByteaArray
		DateArray       pq.StringArray
		TimestampArray  pq.StringArray
		TimestampzArray pq.StringArray
		TimeArray       pq.StringArray
		TimezArray      pq.StringArray

		// array operators
		TextEq               bool
		BoolEq               bool
		TextNeq              bool
		Int4Lt               bool
		Int8Lteq             bool
		DecimalGt            bool
		NumericGtEq          bool
		ByteaContains        bool
		ByteaContainedBy     bool
		DateOverlaps         bool
		TimestampConcat      pq.StringArray
		TimestampzConcatElem pq.StringArray
		TimeAt               time.Time

		Int32EqAny   bool
		DoubleNeqAny bool
		StringEqAll  bool

		// functions
		Append       pq.StringArray
		Cat          pq.StringArray
		Dims         string
		Length       int32
		Lower        int32
		NDims        int32
		Position     *int32
		PositionFrom *int32
		Positions    pq.Int32Array
		Prepend      pq.StringArray
		Remove       pq.BoolArray
		Replace      pq.StringArray
		ToString     string
		Upper        int32
		Cardinality  int32
		Trim         pq.StringArray
	}

	err := query.Query(db, &dest)
	require.NoError(t, err)

	testutils.AssertJSON(t, dest, `
{
	"BoolArray": [
		true,
		false,
		false,
		true
	],
	"Int32Array": [
		-1,
		2,
		-3,
		4,
		5
	],
	"Int64Array": [
		10,
		-11,
		12,
		-13,
		14,
		1515551
	],
	"Float32Array": [
		1.01,
		-2.02,
		3.03,
		4.04
	],
	"Float64Array": [
		10.001,
		20.002,
		-3.003,
		400050.04
	],
	"StringArray": [
		"temp",
		"text",
		"array"
	],
	"ByteaArray": [
		"dGVtcG9yYWw=",
		"Ynl0ZSBhcnJheQ==",
		"YXJyYXk="
	],
	"DateArray": [
		"2022-04-06",
		"2018-05-06"
	],
	"TimestampArray": [
		"1999-01-08 04:05:06"
	],
	"TimestampzArray": [
		"2022-01-01 08:30:50+00"
	],
	"TimeArray": [
		"14:20:45",
		"08:10:15"
	],
	"TimezArray": [
		"04:05:06-08",
		"01:02:03-06"
	],
	"TextEq": true,
	"BoolEq": false,
	"TextNeq": true,
	"Int4Lt": false,
	"Int8Lteq": true,
	"DecimalGt": true,
	"NumericGtEq": true,
	"ByteaContains": false,
	"ByteaContainedBy": false,
	"DateOverlaps": false,
	"TimestampConcat": [
		"2025-01-01 10:00:00",
		"2025-02-01 10:00:00",
		"1999-01-08 04:05:06"
	],
	"TimestampzConcatElem": [
		"2023-02-03 10:29:34.000001+00",
		"1950-02-03 10:30:40+00"
	],
	"TimeAt": "0000-01-01T12:00:00Z",
	"Int32EqAny": false,
	"DoubleNeqAny": true,
	"StringEqAll": false,
	"Append": [
		"alpha",
		"beta",
		"gama",
		"after"
	],
	"Cat": [
		"12:00:00",
		"13:00:00",
		"10:20:30",
		"05:45:45"
	],
	"Dims": "",
	"Length": 2,
	"Lower": 0,
	"NDims": 0,
	"Position": 3,
	"PositionFrom": null,
	"Positions": [
		2
	],
	"Prepend": [
		"before",
		"alpha",
		"beta",
		"gama"
	],
	"Remove": [
		false,
		false
	],
	"Replace": [
		"hi",
		"world"
	],
	"ToString": "happy, ok",
	"Upper": 4,
	"Cardinality": 3,
	"Trim": null
}
`)
	requireLogged(t, query)
}

func TestArraySelectColumnsFromSubQuery(t *testing.T) {

	subQuery := SELECT(
		SampleArrays.AllColumns,
		Int64Array(10, -11, 12, -13, 14, 1515551).AS("int64_array"),
	).FROM(
		SampleArrays,
	).AsTable("sub_query")

	int64Array := IntegerArrayColumn("int64_array").From(subQuery)

	stmt := SELECT(
		subQuery.AllColumns().Except(int64Array),
		int64Array,
	).FROM(
		subQuery,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT sub_query."sample_arrays.id" AS "sample_arrays.id",
     sub_query."sample_arrays.bool_array" AS "sample_arrays.bool_array",
     sub_query."sample_arrays.int2_array_ptr" AS "sample_arrays.int2_array_ptr",
     sub_query."sample_arrays.int4_array" AS "sample_arrays.int4_array",
     sub_query."sample_arrays.int8_array" AS "sample_arrays.int8_array",
     sub_query."sample_arrays.numeric_array" AS "sample_arrays.numeric_array",
     sub_query."sample_arrays.decimal_array" AS "sample_arrays.decimal_array",
     sub_query."sample_arrays.real_array" AS "sample_arrays.real_array",
     sub_query."sample_arrays.double_array" AS "sample_arrays.double_array",
     sub_query."sample_arrays.text_array" AS "sample_arrays.text_array",
     sub_query."sample_arrays.varchar_array" AS "sample_arrays.varchar_array",
     sub_query."sample_arrays.char_array" AS "sample_arrays.char_array",
     sub_query."sample_arrays.bytea_array" AS "sample_arrays.bytea_array",
     sub_query."sample_arrays.date_array" AS "sample_arrays.date_array",
     sub_query."sample_arrays.timestamp_array" AS "sample_arrays.timestamp_array",
     sub_query."sample_arrays.timestamptz_array" AS "sample_arrays.timestamptz_array",
     sub_query."sample_arrays.time_array" AS "sample_arrays.time_array",
     sub_query."sample_arrays.timetz_array" AS "sample_arrays.timetz_array",
     sub_query."sample_arrays.interval_array" AS "sample_arrays.interval_array",
     sub_query."sample_arrays.uuid_array" AS "sample_arrays.uuid_array",
     sub_query."sample_arrays.mood_enum_array" AS "sample_arrays.mood_enum_array",
     sub_query.int64_array AS "int64_array"
FROM (
          SELECT sample_arrays.id AS "sample_arrays.id",
               sample_arrays.bool_array AS "sample_arrays.bool_array",
               sample_arrays.int2_array_ptr AS "sample_arrays.int2_array_ptr",
               sample_arrays.int4_array AS "sample_arrays.int4_array",
               sample_arrays.int8_array AS "sample_arrays.int8_array",
               sample_arrays.numeric_array AS "sample_arrays.numeric_array",
               sample_arrays.decimal_array AS "sample_arrays.decimal_array",
               sample_arrays.real_array AS "sample_arrays.real_array",
               sample_arrays.double_array AS "sample_arrays.double_array",
               sample_arrays.text_array AS "sample_arrays.text_array",
               sample_arrays.varchar_array AS "sample_arrays.varchar_array",
               sample_arrays.char_array AS "sample_arrays.char_array",
               sample_arrays.bytea_array AS "sample_arrays.bytea_array",
               sample_arrays.date_array AS "sample_arrays.date_array",
               sample_arrays.timestamp_array AS "sample_arrays.timestamp_array",
               sample_arrays.timestamptz_array AS "sample_arrays.timestamptz_array",
               sample_arrays.time_array AS "sample_arrays.time_array",
               sample_arrays.timetz_array AS "sample_arrays.timetz_array",
               sample_arrays.interval_array AS "sample_arrays.interval_array",
               sample_arrays.uuid_array AS "sample_arrays.uuid_array",
               sample_arrays.mood_enum_array AS "sample_arrays.mood_enum_array",
               '{10,-11,12,-13,14,1515551}'::bigint[] AS "int64_array"
          FROM test_sample.sample_arrays
     ) AS sub_query;
`)

	var dest struct {
		model.SampleArrays

		Int64Array pq.Int64Array
	}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertDeepEqual(t, dest.SampleArrays, sampleArrayRow0)
	testutils.AssertDeepEqual(t, dest.Int64Array, pq.Int64Array{10, -11, 12, -13, 14, 1515551})
}

func TestArrayTableInsert(t *testing.T) {

	sampleArrayRow3 := testutils.DeepCopy(t, sampleArrayRow0)
	sampleArrayRow3.ID = 3

	insertQuery := SampleArrays.INSERT(SampleArrays.AllColumns).
		VALUES(
			Int64(2),
			ARRAY(Bool(false), Bool(false), Bool(true)),
			ARRAY(Int16(4), Int16(5), Int(6)),
			Int32Array(40, 50, 60),
			Int64Array(400, 500, 600),
			ARRAY(Decimal("1.11"), Decimal("2.22")),
			ARRAY(Decimal("1.11"), Decimal("2.22")),
			Float32Array(4.4, 5.5, 6.6, 7.7),
			Float64Array(40.04, 50.05, 60.06, 70.07),
			StringArray("john", "doe"),
			ARRAY(VarChar(10)("Andy"), VarChar(10)("Bob")),
			ARRAY(Char(1)("q"), Char(1)("w"), Char(1)("e")),
			ByteaArray([]byte("title"), []byte("name")),
			ARRAY(Date(2010, 2, 3), Date(2025, 4, 5)),
			ARRAY(Timestamp(2025, 2, 3, 0, 10, 20, 0)),
			ARRAY(Timestampz(2025, 2, 3, 0, 10, 20, 0, "UTC")),
			ARRAY(Time(12, 15, 45), Time(2, 30, 40)),
			ARRAY(Timez(12, 15, 45, 0, "UTC"), Timez(2, 30, 40, 0, "UTC")),
			ARRAY(INTERVAL(1, DAY, 3, MINUTE), INTERVAL(2, YEAR, 30, DAY)),
			ARRAY(UUID(uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))),
			CAST(ARRAY(enum.Mood.Happy, enum.Mood.Sad)).AS("test_sample.mood[]"),
		).
		MODEL(
			sampleArrayRow3,
		).
		RETURNING(SampleArrays.AllColumns)

	testutils.AssertStatementSql(t, insertQuery, `
INSERT INTO test_sample.sample_arrays (id, bool_array, int2_array_ptr, int4_array, int8_array, numeric_array, decimal_array, real_array, double_array, text_array, varchar_array, char_array, bytea_array, date_array, timestamp_array, timestamptz_array, time_array, timetz_array, interval_array, uuid_array, mood_enum_array)
VALUES ($1::bigint, ARRAY[$2::boolean,$3::boolean,$4::boolean], ARRAY[$5::smallint,$6::smallint,$7], $8::integer[], $9::bigint[], ARRAY[$10::decimal,$11::decimal], ARRAY[$12::decimal,$13::decimal], $14::real[], $15::double precision[], $16::text[], ARRAY[$17::varchar(10),$18::varchar(10)], ARRAY[$19::char(1),$20::char(1),$21::char(1)], $22::bytea[], ARRAY[$23::date,$24::date], ARRAY[$25::timestamp without time zone], ARRAY[$26::timestamp with time zone], ARRAY[$27::time without time zone,$28::time without time zone], ARRAY[$29::time with time zone,$30::time with time zone], ARRAY[INTERVAL '1 DAY 3 MINUTE',INTERVAL '2 YEAR 30 DAY'], ARRAY[$31::uuid], ARRAY['happy','sad']::test_sample.mood[]),
       ($32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52)
RETURNING sample_arrays.id AS "sample_arrays.id",
          sample_arrays.bool_array AS "sample_arrays.bool_array",
          sample_arrays.int2_array_ptr AS "sample_arrays.int2_array_ptr",
          sample_arrays.int4_array AS "sample_arrays.int4_array",
          sample_arrays.int8_array AS "sample_arrays.int8_array",
          sample_arrays.numeric_array AS "sample_arrays.numeric_array",
          sample_arrays.decimal_array AS "sample_arrays.decimal_array",
          sample_arrays.real_array AS "sample_arrays.real_array",
          sample_arrays.double_array AS "sample_arrays.double_array",
          sample_arrays.text_array AS "sample_arrays.text_array",
          sample_arrays.varchar_array AS "sample_arrays.varchar_array",
          sample_arrays.char_array AS "sample_arrays.char_array",
          sample_arrays.bytea_array AS "sample_arrays.bytea_array",
          sample_arrays.date_array AS "sample_arrays.date_array",
          sample_arrays.timestamp_array AS "sample_arrays.timestamp_array",
          sample_arrays.timestamptz_array AS "sample_arrays.timestamptz_array",
          sample_arrays.time_array AS "sample_arrays.time_array",
          sample_arrays.timetz_array AS "sample_arrays.timetz_array",
          sample_arrays.interval_array AS "sample_arrays.interval_array",
          sample_arrays.uuid_array AS "sample_arrays.uuid_array",
          sample_arrays.mood_enum_array AS "sample_arrays.mood_enum_array";
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var dest []model.SampleArrays
		err := insertQuery.Query(tx, &dest)

		require.NoError(t, err)
		require.Len(t, dest, 2)

		testutils.AssertDeepEqual(t, sampleArrayRow3, dest[1])
		testutils.AssertJSON(t, dest[0], `
{
	"ID": 2,
	"BoolArray": [
		false,
		false,
		true
	],
	"Int2ArrayPtr": [
		"4",
		"5",
		"6"
	],
	"Int4Array": [
		40,
		50,
		60
	],
	"Int8Array": [
		400,
		500,
		600
	],
	"NumericArray": [
		1.11,
		2.22
	],
	"DecimalArray": [
		1.11,
		2.22
	],
	"RealArray": [
		4.4,
		5.5,
		6.6,
		7.7
	],
	"DoubleArray": [
		40.04,
		50.05,
		60.06,
		70.07
	],
	"TextArray": [
		"john",
		"doe"
	],
	"VarcharArray": [
		"Andy",
		"Bob"
	],
	"CharArray": [
		"q",
		"w",
		"e"
	],
	"ByteaArray": [
		"dGl0bGU=",
		"bmFtZQ=="
	],
	"DateArray": [
		"2010-02-03",
		"2025-04-05"
	],
	"TimestampArray": [
		"2025-02-03 00:10:20"
	],
	"TimestamptzArray": [
		"2025-02-03 00:10:20+00"
	],
	"TimeArray": [
		"12:15:45",
		"02:30:40"
	],
	"TimetzArray": [
		"12:15:45+00",
		"02:30:40+00"
	],
	"IntervalArray": [
		"1 day 00:03:00",
		"2 years 30 days"
	],
	"UUIDArray": [
		"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	],
	"MoodEnumArray": [
		"happy",
		"sad"
	]
}
`)
	})
}

func TestArrayTableUpdate(t *testing.T) {

	t.Run("using model", func(t *testing.T) {

		sampleArrayRow1Clone := testutils.DeepCopy(t, sampleArrayRow0)
		sampleArrayRow1Clone.RealArray = pq.Float32Array{100.11, 200.22, 300.33}

		stmt := SampleArrays.UPDATE(SampleArrays.MutableColumns).
			MODEL(sampleArrayRow1Clone).
			WHERE(String("alpha").EQ(ANY(SampleArrays.TextArray))).
			RETURNING(SampleArrays.AllColumns)

		testutils.AssertDebugStatementSql(t, stmt, `
UPDATE test_sample.sample_arrays
SET (bool_array, int2_array_ptr, int4_array, int8_array, numeric_array, decimal_array, real_array, double_array, text_array, varchar_array, char_array, bytea_array, date_array, timestamp_array, timestamptz_array, time_array, timetz_array, interval_array, uuid_array, mood_enum_array) = ('{t,f,t}', '{"1","2","3","4"}', '{10,20,30,40}', '{100,200,300,400}', '{1.8881,2.8882,3.8883,4.8884}', '{1.0001,2.0002,3.0003,4.0004}', '{100.11,200.22,300.33}', '{11.11,22.22,33.33}', '{"alpha","beta","gama"}', '{"hello","world"}', '{"a","b","c"}', '{"\\x01020304","\\x11223344"}', '{"2024-11-01","2025-02-28"}', '{"2025-01-01 10:00:00","2025-02-01 10:00:00"}', '{"2025-01-01 09:00:00+00","2025-02-01 09:00:00+00"}', '{"12:00:00","13:00:00"}', '{"12:00:00+01","13:00:00+02"}', '{"1 day","02:00:00"}', '{"550e8400-e29b-41d4-a716-446655440000"}', '{"happy","ok"}')
WHERE 'alpha'::text = ANY(sample_arrays.text_array)
RETURNING sample_arrays.id AS "sample_arrays.id",
          sample_arrays.bool_array AS "sample_arrays.bool_array",
          sample_arrays.int2_array_ptr AS "sample_arrays.int2_array_ptr",
          sample_arrays.int4_array AS "sample_arrays.int4_array",
          sample_arrays.int8_array AS "sample_arrays.int8_array",
          sample_arrays.numeric_array AS "sample_arrays.numeric_array",
          sample_arrays.decimal_array AS "sample_arrays.decimal_array",
          sample_arrays.real_array AS "sample_arrays.real_array",
          sample_arrays.double_array AS "sample_arrays.double_array",
          sample_arrays.text_array AS "sample_arrays.text_array",
          sample_arrays.varchar_array AS "sample_arrays.varchar_array",
          sample_arrays.char_array AS "sample_arrays.char_array",
          sample_arrays.bytea_array AS "sample_arrays.bytea_array",
          sample_arrays.date_array AS "sample_arrays.date_array",
          sample_arrays.timestamp_array AS "sample_arrays.timestamp_array",
          sample_arrays.timestamptz_array AS "sample_arrays.timestamptz_array",
          sample_arrays.time_array AS "sample_arrays.time_array",
          sample_arrays.timetz_array AS "sample_arrays.timetz_array",
          sample_arrays.interval_array AS "sample_arrays.interval_array",
          sample_arrays.uuid_array AS "sample_arrays.uuid_array",
          sample_arrays.mood_enum_array AS "sample_arrays.mood_enum_array";
`)

		testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
			var dest []model.SampleArrays

			err := stmt.Query(tx, &dest)

			require.NoError(t, err)
			require.Len(t, dest, 1)
			testutils.AssertDeepEqual(t, sampleArrayRow1Clone, dest[0])
		})
	})

	t.Run("update using SET", func(t *testing.T) {
		stmt := SampleArrays.UPDATE().
			SET(
				SampleArrays.Int4Array.SET(ARRAY(Int32(-10), Int32(11))),
				SampleArrays.Int8Array.SET(ARRAY(Int64(-1200), Int64(7800))),
			).
			WHERE(String("alpha").EQ(ANY(SampleArrays.TextArray))).
			RETURNING(
				SampleArrays.Int4Array,
				SampleArrays.Int8Array,
			)

		testutils.AssertStatementSql(t, stmt, `
UPDATE test_sample.sample_arrays
SET int4_array = ARRAY[$1::integer,$2::integer],
    int8_array = ARRAY[$3::bigint,$4::bigint]
WHERE $5::text = ANY(sample_arrays.text_array)
RETURNING sample_arrays.int4_array AS "sample_arrays.int4_array",
          sample_arrays.int8_array AS "sample_arrays.int8_array";
`)

		testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
			var dest []model.SampleArrays

			err := stmt.Query(tx, &dest)

			require.NoError(t, err)
			require.Len(t, dest, 1)
			testutils.AssertDeepEqual(t, dest[0].Int4Array, pq.Int32Array{-10, 11})
			testutils.AssertDeepEqual(t, dest[0].Int8Array, pq.Int64Array{-1200, 7800})
		})
	})

}

var sampleArrayRow0 = model.SampleArrays{
	ID:               1,
	BoolArray:        ptr.Of(pq.BoolArray{true, false, true}),
	Int2ArrayPtr:     ptr.Of(pq.StringArray{"1", "2", "3", "4"}),
	Int4Array:        pq.Int32Array{10, 20, 30, 40},
	Int8Array:        pq.Int64Array{100, 200, 300, 400},
	NumericArray:     pq.Float64Array{1.8881, 2.8882, 3.8883, 4.8884},
	DecimalArray:     pq.Float64Array{1.0001, 2.0002, 3.0003, 4.0004},
	RealArray:        pq.Float32Array{1.0099999904632568, 2.0199999809265137, 3.0299999713897705, 4.039999961853027},
	DoubleArray:      pq.Float64Array{11.11, 22.22, 33.33},
	TextArray:        pq.StringArray{"alpha", "beta", "gama"},
	CharArray:        pq.StringArray{"a", "b", "c"},
	VarcharArray:     pq.StringArray{"hello", "world"},
	ByteaArray:       pq.ByteaArray{{0x01, 0x02, 0x03, 0x04}, {0x11, 0x22, 0x33, 0x44}},
	DateArray:        pq.StringArray{"2024-11-01", "2025-02-28"},
	TimestampArray:   &pq.StringArray{"2025-01-01 10:00:00", "2025-02-01 10:00:00"},
	TimestamptzArray: pq.StringArray{"2025-01-01 09:00:00+00", "2025-02-01 09:00:00+00"},
	TimeArray:        pq.StringArray{"12:00:00", "13:00:00"},
	TimetzArray:      pq.StringArray{"12:00:00+01", "13:00:00+02"},
	IntervalArray:    pq.StringArray{"1 day", "02:00:00"},
	UUIDArray:        pq.StringArray{"550e8400-e29b-41d4-a716-446655440000"},
	MoodEnumArray:    pq.StringArray{"happy", "ok"},
}
