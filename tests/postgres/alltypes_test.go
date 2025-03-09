package postgres

import (
	"encoding/base64"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils/ptr"
	"github.com/stretchr/testify/assert"
	"math"

	"github.com/go-jet/jet/v2/qrm"
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

var AllTypesAllColumns = AllTypes.AllColumns.
	Except(IntegerColumn("rowid")) // cockroachDB: exclude rowid column

func TestAllTypesSelect(t *testing.T) {
	var dest []model.AllTypes

	err := AllTypes.SELECT(AllTypesAllColumns).
		LIMIT(2).
		Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertDeepEqual(t, dest[0], allTypesRow0)
	testutils.AssertDeepEqual(t, dest[1], allTypesRow1)
}

func TestAllTypesSelectJson(t *testing.T) {

	stmt := SELECT_JSON_ARR(
		AllTypesAllColumns.Except(
			AllTypes.JSON, AllTypes.JSONPtr,
			AllTypes.Jsonb, AllTypes.JsonbPtr,
			AllTypes.TextArray, AllTypes.TextArrayPtr,
			AllTypes.JsonbArray, AllTypes.IntegerArray, AllTypes.IntegerArrayPtr,
			AllTypes.TextMultiDimArray, AllTypes.TextMultiDimArrayPtr,
		),
		// unsupported at the moment, casting to text allows these columns to be assigned to string fields
		CAST(AllTypes.JSONPtr).AS_TEXT().AS("jsonPtr"),
		CAST(AllTypes.JSON).AS_TEXT().AS("JSON"),
		CAST(AllTypes.JsonbPtr).AS_TEXT().AS("jsonbPtr"),
		CAST(AllTypes.Jsonb).AS_TEXT().AS("Jsonb"),
		CAST(AllTypes.TextArrayPtr).AS_TEXT().AS("TextArrayPtr"),
		CAST(AllTypes.TextArray).AS_TEXT().AS("TextArray"),
		CAST(AllTypes.JsonbArray).AS_TEXT().AS("JsonbArray"),
		CAST(AllTypes.IntegerArray).AS_TEXT().AS("IntegerArray"),
		CAST(AllTypes.IntegerArrayPtr).AS_TEXT().AS("IntegerArrayPtr"),
		CAST(AllTypes.TextMultiDimArray).AS_TEXT().AS("TextMultiDimArray"),
		CAST(AllTypes.TextMultiDimArrayPtr).AS_TEXT().AS("TextMultiDimArrayPtr"),
	).FROM(AllTypes)

	testutils.AssertStatementSql(t, stmt, `
SELECT json_agg(row_to_json(records)) AS "json"
FROM (
          SELECT all_types.small_int_ptr AS "smallIntPtr",
               all_types.small_int AS "smallInt",
               all_types.integer_ptr AS "integerPtr",
               all_types.integer AS "integer",
               all_types.big_int_ptr AS "bigIntPtr",
               all_types.big_int AS "bigInt",
               all_types.decimal_ptr AS "decimalPtr",
               all_types.decimal AS "decimal",
               all_types.numeric_ptr AS "numericPtr",
               all_types.numeric AS "numeric",
               all_types.real_ptr AS "realPtr",
               all_types.real AS "real",
               all_types.double_precision_ptr AS "doublePrecisionPtr",
               all_types.double_precision AS "doublePrecision",
               all_types.smallserial AS "smallserial",
               all_types.serial AS "serial",
               all_types.bigserial AS "bigserial",
               all_types.var_char_ptr AS "varCharPtr",
               all_types.var_char AS "varChar",
               all_types.char_ptr AS "charPtr",
               all_types.char AS "char",
               all_types.text_ptr AS "textPtr",
               all_types.text AS "text",
               ENCODE(all_types.bytea_ptr, 'base64') AS "byteaPtr",
               ENCODE(all_types.bytea, 'base64') AS "bytea",
               all_types.timestampz_ptr AS "timestampzPtr",
               all_types.timestampz AS "timestampz",
               to_char(all_types.timestamp_ptr, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "timestampPtr",
               to_char(all_types.timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "timestamp",
               to_char(all_types.date_ptr::timestamp, 'YYYY-MM-DD') || 'T00:00:00Z' AS "datePtr",
               to_char(all_types.date::timestamp, 'YYYY-MM-DD') || 'T00:00:00Z' AS "date",
               '0000-01-01T' || to_char('2000-10-10'::date + all_types.timez_ptr, 'HH24:MI:SS.USTZH:TZM') AS "timezPtr",
               '0000-01-01T' || to_char('2000-10-10'::date + all_types.timez, 'HH24:MI:SS.USTZH:TZM') AS "timez",
               '0000-01-01T' || to_char('2000-10-10'::date + all_types.time_ptr, 'HH24:MI:SS.USZ') AS "timePtr",
               '0000-01-01T' || to_char('2000-10-10'::date + all_types.time, 'HH24:MI:SS.USZ') AS "time",
               all_types.interval_ptr AS "intervalPtr",
               all_types.interval AS "interval",
               all_types.boolean_ptr AS "booleanPtr",
               all_types.boolean AS "boolean",
               all_types.point_ptr AS "pointPtr",
               all_types.bit_ptr AS "bitPtr",
               all_types.bit AS "bit",
               all_types.bit_varying_ptr AS "bitVaryingPtr",
               all_types.bit_varying AS "bitVarying",
               all_types.tsvector_ptr AS "tsvectorPtr",
               all_types.tsvector AS "tsvector",
               all_types.uuid_ptr AS "uuidPtr",
               all_types.uuid AS "uuid",
               all_types.xml_ptr AS "xmlPtr",
               all_types.xml AS "xml",
               all_types.mood_ptr AS "moodPtr",
               all_types.mood AS "mood",
               all_types.json_ptr::text AS "jsonPtr",
               all_types.json::text AS "JSON",
               all_types.jsonb_ptr::text AS "jsonbPtr",
               all_types.jsonb::text AS "Jsonb",
               all_types.text_array_ptr::text AS "TextArrayPtr",
               all_types.text_array::text AS "TextArray",
               all_types.jsonb_array::text AS "JsonbArray",
               all_types.integer_array::text AS "IntegerArray",
               all_types.integer_array_ptr::text AS "IntegerArrayPtr",
               all_types.text_multi_dim_array::text AS "TextMultiDimArray",
               all_types.text_multi_dim_array_ptr::text AS "TextMultiDimArrayPtr"
          FROM test_sample.all_types
     ) AS records;
`)

	var dest []model.AllTypes

	err := stmt.QueryContext(ctx, db, &dest)
	require.NoError(t, err)

	// fix inconsistencies between postgres and cockroachdb.
	// cockroachdb returns char[N] columns with trailing whitespaces trimmed
	if sourceIsCockroachDB() {
		dest[0].Char = allTypesRow0.Char
		dest[0].CharPtr = allTypesRow0.CharPtr

		dest[1].Char = allTypesRow1.Char
		dest[1].CharPtr = allTypesRow1.CharPtr
	}

	minus8 := time.FixedZone("UTC", -8*60*60)
	plus1 := time.FixedZone("UTC", 60*60)

	// set time local before comparison
	dest[0].Timez = *toTZ(&dest[0].Timez, minus8)
	dest[0].TimezPtr = toTZ(dest[0].TimezPtr, minus8)
	dest[1].Timez = *toTZ(&dest[1].Timez, minus8)
	dest[1].TimezPtr = toTZ(dest[1].TimezPtr, minus8)

	dest[0].Timestampz = *toTZ(&dest[0].Timestampz, plus1)
	dest[0].TimestampzPtr = toTZ(dest[0].TimestampzPtr, plus1)
	dest[1].Timestampz = *toTZ(&dest[1].Timestampz, plus1)
	dest[1].TimestampzPtr = toTZ(dest[1].TimestampzPtr, plus1)

	testutils.AssertJsonEqual(t, dest[0], allTypesRow0)
	testutils.AssertJsonEqual(t, dest[1], allTypesRow1)
}

func toTZ(tm *time.Time, loc *time.Location) *time.Time {
	if tm == nil {
		return nil
	}

	return ptr.Of(tm.In(loc))
}

func TestAllTypesViewSelect(t *testing.T) {
	type AllTypesView model.AllTypes
	var dest []AllTypesView

	err := SELECT(view.AllTypesView.AllColumns).
		FROM(view.AllTypesView).
		Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertDeepEqual(t, dest[0], AllTypesView(allTypesRow0))
	testutils.AssertDeepEqual(t, dest[1], AllTypesView(allTypesRow1))
}

func TestMaterializedViewAllTypes(t *testing.T) {
	stmt := SELECT(
		view.AllTypesMaterializedView.AllColumns.
			Except(IntegerColumn("rowid")), // cockroachDB: exclude rowid column
	).FROM(
		view.AllTypesMaterializedView,
	)

	type AllTypesMaterializedView model.AllTypes
	var dest []AllTypesMaterializedView

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertDeepEqual(t, dest[0], AllTypesMaterializedView(allTypesRow0))
	testutils.AssertDeepEqual(t, dest[1], AllTypesMaterializedView(allTypesRow1))
}

func TestAllTypesInsertModel(t *testing.T) {
	skipForPgxDriver(t) // pgx driver bug ERROR: date/time field value out of range: "0000-01-01 12:05:06Z" (SQLSTATE 22008)

	query := AllTypes.INSERT(AllTypesAllColumns).
		MODEL(allTypesRow0).
		MODEL(&allTypesRow1).
		RETURNING(AllTypes.AllColumns)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var dest []model.AllTypes
		err := query.Query(tx, &dest)
		require.NoError(t, err)

		if sourceIsCockroachDB() {
			return
		}
		require.Equal(t, len(dest), 2)
		testutils.AssertDeepEqual(t, dest[0], allTypesRow0)
		testutils.AssertDeepEqual(t, dest[1], allTypesRow1)
	})
}

func TestAllTypesInsertQuery(t *testing.T) {
	query := AllTypes.INSERT(AllTypesAllColumns).
		QUERY(
			AllTypes.
				SELECT(AllTypesAllColumns).
				LIMIT(2),
		).
		RETURNING(AllTypesAllColumns)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var dest []model.AllTypes
		err := query.Query(tx, &dest)

		require.NoError(t, err)
		require.Equal(t, len(dest), 2)
		testutils.AssertDeepEqual(t, dest[0], allTypesRow0)
		testutils.AssertDeepEqual(t, dest[1], allTypesRow1)
	})
}

func TestUUIDType(t *testing.T) {
	id := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")

	query := AllTypes.
		SELECT(AllTypes.UUID, AllTypes.UUIDPtr).
		WHERE(AllTypes.UUID.EQ(UUID(id)))

	testutils.AssertDebugStatementSql(t, query, `
SELECT all_types.uuid AS "all_types.uuid",
     all_types.uuid_ptr AS "all_types.uuid_ptr"
FROM test_sample.all_types
WHERE all_types.uuid = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';
`, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")

	result := model.AllTypes{}

	err := query.Query(db, &result)
	require.NoError(t, err)
	require.Equal(t, result.UUID, uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
	testutils.AssertDeepEqual(t, result.UUIDPtr, testutils.UUIDPtr("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
	requireLogged(t, query)
}

func TestByteaInsert(t *testing.T) {
	byteArrHex := "\\x48656c6c6f20476f7068657221"
	byteArrBin := []byte("\x48\x65\x6c\x6c\x6f\x20\x47\x6f\x70\x68\x65\x72\x21")

	insertStmt := AllTypes.INSERT(AllTypes.Bytea, AllTypes.ByteaPtr).
		VALUES(byteArrHex, byteArrBin).
		RETURNING(AllTypes.Bytea, AllTypes.ByteaPtr)

	testutils.AssertStatementSql(t, insertStmt, `
INSERT INTO test_sample.all_types (bytea, bytea_ptr)
VALUES ($1, $2)
RETURNING all_types.bytea AS "all_types.bytea",
          all_types.bytea_ptr AS "all_types.bytea_ptr";
`, byteArrHex, byteArrBin)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var inserted model.AllTypes
		err := insertStmt.Query(tx, &inserted)
		require.NoError(t, err)

		require.Equal(t, string(*inserted.ByteaPtr), "Hello Gopher!")
		// It is not possible to initiate bytea column using hex format '\xDEADBEEF' with pq driver.
		// pq driver always encodes parameter string if destination column is of type bytea.
		// Probably pq driver error.
		// require.Equal(t, string(inserted.Bytea), "Hello Gopher!")

		stmt := SELECT(
			AllTypes.Bytea,
			AllTypes.ByteaPtr,
		).FROM(
			AllTypes,
		).WHERE(
			AllTypes.ByteaPtr.EQ(Bytea(byteArrBin)),
		)

		testutils.AssertStatementSql(t, stmt, `
SELECT all_types.bytea AS "all_types.bytea",
     all_types.bytea_ptr AS "all_types.bytea_ptr"
FROM test_sample.all_types
WHERE all_types.bytea_ptr = $1::bytea;
`, byteArrBin)

		var dest model.AllTypes

		err = stmt.Query(tx, &dest)
		require.NoError(t, err)

		require.Equal(t, string(*dest.ByteaPtr), "Hello Gopher!")
		// Probably pq driver error.
		// require.Equal(t, string(dest.Bytea), "Hello Gopher!")
	})
}

func TestAllTypesFromSubQuery(t *testing.T) {
	subQuery := SELECT(AllTypesAllColumns).
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
     "allTypesSubQuery"."all_types.text_multi_dim_array" AS "all_types.text_multi_dim_array",
     "allTypesSubQuery"."all_types.mood_ptr" AS "all_types.mood_ptr",
     "allTypesSubQuery"."all_types.mood" AS "all_types.mood"
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
               all_types.text_multi_dim_array AS "all_types.text_multi_dim_array",
               all_types.mood_ptr AS "all_types.mood_ptr",
               all_types.mood AS "all_types.mood"
          FROM test_sample.all_types
     ) AS "allTypesSubQuery"
LIMIT 2;
`)

	var dest []model.AllTypes
	err := mainQuery.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 2)
}

func TestExpressionOperators(t *testing.T) {
	query := AllTypes.SELECT(
		AllTypes.Integer.IS_NULL().AS("result.is_null"),
		AllTypes.DatePtr.IS_NOT_NULL().AS("result.is_not_null"),
		AllTypes.SmallIntPtr.IN(Int8(11), Int8(22)).AS("result.in"),
		AllTypes.SmallIntPtr.IN(AllTypes.SELECT(AllTypes.Integer)).AS("result.in_select"),

		Raw("CURRENT_USER").AS("result.raw"),
		Raw("#1 + COALESCE(all_types.small_int_ptr, 0) + #2", RawArgs{"#1": 78, "#2": 56}).AS("result.raw_arg"),
		Raw("#1 + all_types.integer + #2 + #1 + #3 + #4",
			RawArgs{"#1": 11, "#2": 22, "#3": 33, "#4": 44}).AS("result.raw_arg2"),

		AllTypes.SmallIntPtr.NOT_IN(Int(11), Int16(22), NULL).AS("result.not_in"),
		AllTypes.SmallIntPtr.NOT_IN(AllTypes.SELECT(AllTypes.Integer)).AS("result.not_in_select"),
	).LIMIT(2)

	testutils.AssertStatementSql(t, query, `
SELECT all_types.integer IS NULL AS "result.is_null",
     all_types.date_ptr IS NOT NULL AS "result.is_not_null",
     (all_types.small_int_ptr IN ($1::smallint, $2::smallint)) AS "result.in",
     (all_types.small_int_ptr IN ((
          SELECT all_types.integer AS "all_types.integer"
          FROM test_sample.all_types
     ))) AS "result.in_select",
     (CURRENT_USER) AS "result.raw",
     ($3 + COALESCE(all_types.small_int_ptr, 0) + $4) AS "result.raw_arg",
     ($5 + all_types.integer + $6 + $5 + $7 + $8) AS "result.raw_arg2",
     (all_types.small_int_ptr NOT IN ($9, $10::smallint, NULL)) AS "result.not_in",
     (all_types.small_int_ptr NOT IN ((
          SELECT all_types.integer AS "all_types.integer"
          FROM test_sample.all_types
     ))) AS "result.not_in_select"
FROM test_sample.all_types
LIMIT $11;
`, int8(11), int8(22), 78, 56, 11, 22, 33, 44, int64(11), int16(22), int64(2))

	var dest []struct {
		common.ExpressionTestResult `alias:"result.*"`
	}

	err := query.Query(db, &dest)

	require.NoError(t, err)
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
	skipForPgxDriver(t) // pgx driver bug 'cannot convert 151 to Text'

	query := AllTypes.SELECT(
		CAST(Int(151)).AS_CHAR(12).AS("char12"),
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
		CAST(String("04:05:06+01:00")).AS_TIMEZ(),
		CAST(String("1999-01-08 04:05:06")).AS_TIMESTAMP(),
		CAST(String("1999-01-08 04:05:06+01:00")).AS_TIMESTAMPZ(),
		CAST(String("04:05:06")).AS_INTERVAL(),
		CAST(String("some text")).AS_BYTEA().EQ(Bytea([]byte("some text"))),

		func() ProjectionList {
			if sourceIsCockroachDB() {
				return ProjectionList{NULL}
			}

			// cockroach doesn't support currently
			return ProjectionList{
				TO_CHAR(AllTypes.Timestamp, String("HH12:MI:SS")),
				TO_CHAR(AllTypes.Integer, String("999")),
				TO_CHAR(AllTypes.DoublePrecision, String("999D9")),
				TO_CHAR(AllTypes.Numeric, String("999D99S")),

				TO_DATE(String("05 Dec 2000"), String("DD Mon YYYY")),
				TO_NUMBER(String("12,454"), String("99G999D9S")),
				TO_TIMESTAMP(String("05 Dec 2000"), String("DD Mon YYYY")),
			}
		}(),

		COALESCE(AllTypes.IntegerPtr, AllTypes.SmallIntPtr, NULL, Int(11)),
		NULLIF(AllTypes.Text, String("(none)")),
		GREATEST(AllTypes.Numeric, AllTypes.NumericPtr),
		LEAST(AllTypes.Numeric, AllTypes.NumericPtr),

		Raw("current_database()"),
	)

	var dest []struct{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestStringOperators(t *testing.T) {
	skipForCockroachDB(t) // some string functions are still unimplemented
	skipForPgxDriver(t)   // pgx driver bug 'cannot convert 11 to Text'

	query := AllTypes.SELECT(
		AllTypes.Text.EQ(AllTypes.Char),
		AllTypes.Text.EQ(Text("Text")),
		AllTypes.Text.NOT_EQ(AllTypes.VarCharPtr),
		AllTypes.Text.NOT_EQ(Text("Text")),
		AllTypes.Text.GT(AllTypes.Text),
		AllTypes.Text.GT(Text("Text")),
		AllTypes.Text.GT_EQ(AllTypes.TextPtr),
		AllTypes.Text.GT_EQ(Text("Text")),
		AllTypes.Text.LT(AllTypes.Char),
		AllTypes.Text.LT(Text("Text")),
		AllTypes.Text.LT_EQ(AllTypes.VarChar),
		AllTypes.Text.LT_EQ(String("Text")),
		AllTypes.Text.BETWEEN(String("min"), String("max")),
		AllTypes.Text.NOT_BETWEEN(AllTypes.VarChar, AllTypes.CharPtr),
		AllTypes.Text.CONCAT(String("text2")),
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
		LOWER(Char(4)("length")),
		UPPER(AllTypes.Char),
		UPPER(VarChar()("upper")),
		BTRIM(AllTypes.VarChar),
		BTRIM(Char()("btrim")),
		BTRIM(AllTypes.VarChar, String("AA")),
		BTRIM(VarChar(11)("btrim"), String("AA")),
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
		CONVERT(Bytea("bytea"), UTF8, LATIN1),
		CONVERT(AllTypes.Bytea, UTF8, LATIN1),
		CONVERT_FROM(Bytea("text_in_utf8"), UTF8),
		CONVERT_TO(String("text_in_utf8"), UTF8),
		ENCODE(Bytea("some text"), Escape),
		DECODE(String("MTIzAAE="), Base64),
		FORMAT(String("Hello %s, %1$s"), String("World")),
		INITCAP(String("hi THOMAS")),
		LEFT(String("abcde"), Int(2)),
		RIGHT(String("abcde"), Int(2)),
		LENGTH(Bytea("jose")),
		LENGTH(Bytea("jose"), UTF8),
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

	var dest []struct{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestBytea(t *testing.T) {

	var sampleBytea = Bytea([]byte{11, 0, 22, 33, 44})
	var textBytea = Bytea([]byte("text blob"))

	stmt := SELECT(
		AllTypes.Bytea.EQ(sampleBytea),
		AllTypes.Bytea.EQ(AllTypes.ByteaPtr),
		AllTypes.Bytea.NOT_EQ(sampleBytea),
		AllTypes.Bytea.GT(textBytea),
		AllTypes.Bytea.GT_EQ(AllTypes.ByteaPtr),
		AllTypes.Bytea.LT(AllTypes.ByteaPtr),
		AllTypes.Bytea.LT_EQ(sampleBytea),
		AllTypes.Bytea.BETWEEN(Bytea([]byte("min")), Bytea([]byte("max"))),
		AllTypes.Bytea.NOT_BETWEEN(AllTypes.Bytea, AllTypes.ByteaPtr),
		AllTypes.Bytea.CONCAT(textBytea),

		func() ProjectionList {
			if sourceIsCockroachDB() {
				return ProjectionList{NULL}
			}
			// cockroach doesn't support currently
			return ProjectionList{
				AllTypes.Bytea.LIKE(Bytea("b'%pattern%'")),
				AllTypes.Bytea.NOT_LIKE(Bytea("b'%pattern%'")),

				BTRIM(AllTypes.Bytea, Bytea([]byte{33})),
				RTRIM(AllTypes.ByteaPtr, sampleBytea),
				LTRIM(sampleBytea, textBytea),
				CONCAT(sampleBytea, AllTypes.ByteaPtr, textBytea),
				BIT_COUNT(sampleBytea).EQ(Int(3)),
				LENGTH(textBytea, UTF8).EQ(Int(4)),

				CONVERT(textBytea, UTF8, WIN1252),
				CONVERT(AllTypes.Bytea, UTF8, LATIN1).EQ(sampleBytea),
			}
		}(),

		BIT_LENGTH(textBytea),
		OCTET_LENGTH(textBytea),

		GET_BIT(textBytea, Int(2)).EQ(Int(23)),
		GET_BYTE(sampleBytea, Int(1)).EQ(Int(0)),
		SET_BIT(textBytea, Int(1), Int(0)).EQ(sampleBytea),
		SET_BYTE(textBytea, Int(1), Int(0)).EQ(textBytea),
		LENGTH(sampleBytea),

		SUBSTR(AllTypes.Bytea, Int(0), Int(2)),

		MD5(AllTypes.Bytea),
		SHA224(AllTypes.Bytea),
		SHA256(AllTypes.Bytea),
		SHA384(AllTypes.Bytea),
		SHA512(AllTypes.Bytea),

		ENCODE(sampleBytea, Base64),
		DECODE(String("A234C12B"), Hex).EQ(sampleBytea),

		CONVERT_FROM(AllTypes.ByteaPtr, UTF8).EQ(AllTypes.VarChar),
		CONVERT_TO(AllTypes.Text, UTF8).NOT_EQ(textBytea),

		RawBytea("DECODE(#1::text, #2)", RawArgs{
			"#1": "A234C12B",
			"#2": "hex",
		}).EQ(sampleBytea),
	).FROM(
		AllTypes,
	)

	if !sourceIsCockroachDB() {
		testutils.AssertStatementSql(t, stmt, `
SELECT all_types.bytea = $1::bytea,
     all_types.bytea = all_types.bytea_ptr,
     all_types.bytea != $2::bytea,
     all_types.bytea > $3::bytea,
     all_types.bytea >= all_types.bytea_ptr,
     all_types.bytea < all_types.bytea_ptr,
     all_types.bytea <= $4::bytea,
     all_types.bytea BETWEEN $5::bytea AND $6::bytea,
     all_types.bytea NOT BETWEEN all_types.bytea AND all_types.bytea_ptr,
     all_types.bytea || $7::bytea,
     all_types.bytea LIKE $8::bytea,
     all_types.bytea NOT LIKE $9::bytea,
     BTRIM(all_types.bytea, $10::bytea),
     RTRIM(all_types.bytea_ptr, $11::bytea),
     LTRIM($12::bytea, $13::bytea),
     CONCAT($14::bytea, all_types.bytea_ptr, $15::bytea),
     BIT_COUNT($16::bytea) = $17,
     LENGTH($18::bytea, 'UTF8') = $19,
     CONVERT($20::bytea, 'UTF8', 'WIN1252'),
     CONVERT(all_types.bytea, 'UTF8', 'LATIN1') = $21::bytea,
     BIT_LENGTH($22::bytea),
     OCTET_LENGTH($23::bytea),
     GET_BIT($24::bytea, $25) = $26,
     GET_BYTE($27::bytea, $28) = $29,
     SET_BIT($30::bytea, $31, $32) = $33::bytea,
     SET_BYTE($34::bytea, $35, $36) = $37::bytea,
     LENGTH($38::bytea),
     SUBSTR(all_types.bytea, $39, $40),
     MD5(all_types.bytea),
     SHA224(all_types.bytea),
     SHA256(all_types.bytea),
     SHA384(all_types.bytea),
     SHA512(all_types.bytea),
     ENCODE($41::bytea, 'base64'),
     DECODE($42::text, 'hex') = $43::bytea,
     CONVERT_FROM(all_types.bytea_ptr, 'UTF8') = all_types.var_char,
     CONVERT_TO(all_types.text, 'UTF8') != $44::bytea,
     (DECODE($45::text, $46)) = $47::bytea
FROM test_sample.all_types;
`)
	}

	var dest []struct{}
	err := stmt.Query(db, &dest)

	require.NoError(t, err)
}

func TestBlobConversion(t *testing.T) {

	nonPrintable := []byte{11, 22, 33, 44, 55}
	printable := []byte("this is blob")

	stmt := SELECT(
		Bytea(nonPrintable).AS("test_dest.non_printable"),
		Bytea(printable).AS("test_dest.printable"),

		Bytea(nonPrintable).CONCAT(Bytea(printable)).AS("test_dest.bytea_concat"),

		ENCODE(Bytea(nonPrintable), Base64).AS("test_dest.non_printable_base64"),
		CONVERT_FROM(Bytea(printable), UTF8).AS("test_dest.printable_utf8"),
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT '\x0b16212c37'::bytea AS "test_dest.non_printable",
     '\x7468697320697320626c6f62'::bytea AS "test_dest.printable",
     ('\x0b16212c37'::bytea || '\x7468697320697320626c6f62'::bytea) AS "test_dest.bytea_concat",
     ENCODE('\x0b16212c37'::bytea, 'base64') AS "test_dest.non_printable_base64",
     CONVERT_FROM('\x7468697320697320626c6f62'::bytea, 'UTF8') AS "test_dest.printable_utf8";
`)

	type testDest struct {
		NonPrintable []byte
		Printable    []byte

		ByteaConcat        []byte
		NonPrintableBase64 string
		PrintableUTF8      string
	}

	var dest testDest

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, dest.NonPrintable, nonPrintable)
	require.Equal(t, dest.Printable, printable)

	require.Equal(t, dest.ByteaConcat, append(nonPrintable, printable...))
	require.Equal(t, dest.NonPrintableBase64, base64.StdEncoding.EncodeToString(nonPrintable))
	require.Equal(t, dest.PrintableUTF8, string(printable))

	t.Run("using select json", func(t *testing.T) {
		stmtJson := SELECT_JSON_OBJ(
			Bytea(nonPrintable).AS("nonPrintable"),
			Bytea(printable).AS("printable"),

			Bytea(nonPrintable).CONCAT(Bytea(printable)).AS("byteaConcat"),

			ENCODE(Bytea(nonPrintable), Base64).AS("nonPrintableBase64"),
			CONVERT_FROM(Bytea(printable), UTF8).AS("printableUtf8"),
		)

		testutils.AssertStatementSql(t, stmtJson, `
SELECT row_to_json(records) AS "json"
FROM (
          SELECT ENCODE($1::bytea, 'base64') AS "nonPrintable",
               ENCODE($2::bytea, 'base64') AS "printable",
               ENCODE($3::bytea || $4::bytea, 'base64') AS "byteaConcat",
               ENCODE($5::bytea, 'base64') AS "nonPrintableBase64",
               CONVERT_FROM($6::bytea, 'UTF8') AS "printableUtf8"
     ) AS records;
`)

		var destSelectJson testDest

		err := stmtJson.QueryContext(ctx, db, &destSelectJson)
		require.NoError(t, err)
		testutils.PrintJson(destSelectJson)

		require.Equal(t, dest, destSelectJson)

	})
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
     (all_types.boolean = $1::boolean) AS "EQ2",
     (all_types.boolean != all_types.boolean_ptr) AS "NEq1",
     (all_types.boolean != $2::boolean) AS "NEq2",
     (all_types.boolean IS DISTINCT FROM all_types.boolean_ptr) AS "distinct1",
     (all_types.boolean IS DISTINCT FROM $3::boolean) AS "distinct2",
     (all_types.boolean IS NOT DISTINCT FROM all_types.boolean_ptr) AS "not_distinct_1",
     (all_types.boolean IS NOT DISTINCT FROM $4::boolean) AS "NOTDISTINCT2",
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
	skipForCockroachDB(t) // some functions are still unimplemented

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

		TRUNC(AllTypes.Decimal.ADD(AllTypes.Decimal), Int8(2)).AS("add1"),
		TRUNC(AllTypes.Decimal.ADD(Float(11.22)), Int8(2)).AS("add2"),
		TRUNC(AllTypes.Decimal.SUB(AllTypes.DecimalPtr), Int32(2)).AS("sub1"),
		TRUNC(AllTypes.Decimal.SUB(Float(11.22)), Int16(2)).AS("sub2"),
		TRUNC(AllTypes.Decimal.MUL(AllTypes.DecimalPtr), Int16(2)).AS("mul1"),
		TRUNC(AllTypes.Decimal.MUL(Float(11.22)), Int32(2)).AS("mul2"),
		TRUNC(AllTypes.Decimal.DIV(AllTypes.DecimalPtr), Int32(2)).AS("div1"),
		TRUNC(AllTypes.Decimal.DIV(Float(11.22)), Int8(2)).AS("div2"),
		TRUNC(AllTypes.Decimal.MOD(AllTypes.DecimalPtr), Int8(2)).AS("mod1"),
		TRUNC(AllTypes.Decimal.MOD(Float(11.22)), Int8(2)).AS("mod2"),
		TRUNC(AllTypes.Decimal.POW(AllTypes.DecimalPtr), Int8(2)).AS("pow1"),
		TRUNC(AllTypes.Decimal.POW(Float(2.1)), Int8(2)).AS("pow2"),

		TRUNC(ABSf(AllTypes.Decimal), Int8(2)).AS("abs"),
		TRUNC(POWER(AllTypes.Decimal, Float(2.1)), Int8(2)).AS("power"),
		TRUNC(SQRT(AllTypes.Decimal), Int16(2)).AS("sqrt"),
		TRUNC(CAST(CBRT(AllTypes.Decimal)).AS_DECIMAL(), Int8(2)).AS("cbrt"),

		CEIL(AllTypes.Real).AS("ceil"),
		FLOOR(AllTypes.Real).AS("floor"),
		ROUND(AllTypes.Decimal).AS("round1"),
		ROUND(AllTypes.Decimal, AllTypes.Integer).AS("round2"),

		SIGN(AllTypes.Real).AS("sign"),
		TRUNC(AllTypes.Decimal, Int32(1)).AS("trunc"),
	).LIMIT(2)

	//fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, `
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
     (all_types.numeric BETWEEN $11 AND all_types.decimal) AS "between",
     (all_types.numeric NOT BETWEEN (all_types.decimal * $12) AND $13) AS "not_between",
     TRUNC(all_types.decimal + all_types.decimal, $14::smallint) AS "add1",
     TRUNC(all_types.decimal + $15, $16::smallint) AS "add2",
     TRUNC(all_types.decimal - all_types.decimal_ptr, $17::integer) AS "sub1",
     TRUNC(all_types.decimal - $18, $19::smallint) AS "sub2",
     TRUNC(all_types.decimal * all_types.decimal_ptr, $20::smallint) AS "mul1",
     TRUNC(all_types.decimal * $21, $22::integer) AS "mul2",
     TRUNC(all_types.decimal / all_types.decimal_ptr, $23::integer) AS "div1",
     TRUNC(all_types.decimal / $24, $25::smallint) AS "div2",
     TRUNC(all_types.decimal % all_types.decimal_ptr, $26::smallint) AS "mod1",
     TRUNC(all_types.decimal % $27, $28::smallint) AS "mod2",
     TRUNC(POW(all_types.decimal, all_types.decimal_ptr), $29::smallint) AS "pow1",
     TRUNC(POW(all_types.decimal, $30), $31::smallint) AS "pow2",
     TRUNC(ABS(all_types.decimal), $32::smallint) AS "abs",
     TRUNC(POWER(all_types.decimal, $33), $34::smallint) AS "power",
     TRUNC(SQRT(all_types.decimal), $35::smallint) AS "sqrt",
     TRUNC(CBRT(all_types.decimal)::decimal, $36::smallint) AS "cbrt",
     CEIL(all_types.real) AS "ceil",
     FLOOR(all_types.real) AS "floor",
     ROUND(all_types.decimal) AS "round1",
     ROUND(all_types.decimal, all_types.integer) AS "round2",
     SIGN(all_types.real) AS "sign",
     TRUNC(all_types.decimal, $37::integer) AS "trunc"
FROM test_sample.all_types
LIMIT $38;
`)

	var dest []struct {
		common.FloatExpressionTestResult `alias:"."`
	}

	err := query.Query(db, &dest)

	require.NoError(t, err)

	//testutils.PrintJson(dest)
	// testutils.SaveJSONFile(dest, "./testdata/results/common/float_operators.json")

	testutils.AssertJSONFile(t, dest, "./testdata/results/common/float_operators.json")
}

func TestUInt64Overflow(t *testing.T) {
	stmt := AllTypes.INSERT(AllTypes.BigInt).
		VALUES(Uint64(math.MaxUint64))

	_, err := stmt.Exec(db)
	if isPgxDriver() {
		require.ErrorContains(t, err, "18446744073709551615 is greater than maximum value for Int8")
	} else {
		require.ErrorContains(t, err, "sql: converting argument $1 type: uint64 values with high bit set are not supported")
	}
}

func TestIntegerOperators(t *testing.T) {
	skipForCockroachDB(t) // some functions are still unimplemented

	query := AllTypes.SELECT(
		AllTypes.BigInt,
		AllTypes.BigIntPtr,
		AllTypes.SmallInt,
		AllTypes.SmallIntPtr,

		AllTypes.BigInt.EQ(AllTypes.BigInt).AS("eq1"),
		AllTypes.BigInt.EQ(Int64(12)).AS("eq2"),
		AllTypes.BigInt.NOT_EQ(AllTypes.BigIntPtr).AS("neq1"),
		AllTypes.BigInt.NOT_EQ(Int64(12)).AS("neq2"),
		AllTypes.BigInt.IS_DISTINCT_FROM(AllTypes.BigInt).AS("distinct1"),
		AllTypes.BigInt.IS_DISTINCT_FROM(Int32(12)).AS("distinct2"),
		AllTypes.BigInt.IS_NOT_DISTINCT_FROM(AllTypes.BigInt).AS("not distinct1"),
		AllTypes.BigInt.IS_NOT_DISTINCT_FROM(Int32(12)).AS("not distinct2"),

		AllTypes.Integer.BETWEEN(Int(11), Int(200)).AS("between"),
		AllTypes.Integer.NOT_BETWEEN(Int(66), Int(77)).AS("not_between"),
		AllTypes.BigInt.LT(AllTypes.BigIntPtr).AS("lt1"),
		AllTypes.BigInt.LT(Int16(65)).AS("lt2"),
		AllTypes.BigInt.LT_EQ(AllTypes.BigIntPtr).AS("lte1"),
		AllTypes.BigInt.LT_EQ(Int32(65)).AS("lte2"),
		AllTypes.BigInt.GT(AllTypes.BigIntPtr).AS("gt1"),
		AllTypes.BigInt.GT(Int64(65)).AS("gt2"),
		AllTypes.BigInt.GT_EQ(AllTypes.BigIntPtr).AS("gte1"),
		AllTypes.BigInt.GT_EQ(Int64(65)).AS("gte2"),

		AllTypes.BigInt.ADD(AllTypes.BigInt).AS("add1"),
		AllTypes.BigInt.ADD(Int(11)).AS("add2"),
		AllTypes.BigInt.SUB(AllTypes.BigInt).AS("sub1"),
		AllTypes.BigInt.SUB(Int8(11)).AS("sub2"),
		AllTypes.BigInt.MUL(AllTypes.BigInt).AS("mul1"),
		AllTypes.BigInt.MUL(Int16(11)).AS("mul2"),
		AllTypes.BigInt.DIV(AllTypes.BigInt).AS("div1"),
		AllTypes.BigInt.DIV(Int32(11)).AS("div2"),
		AllTypes.BigInt.MOD(AllTypes.BigInt).AS("mod1"),
		AllTypes.BigInt.MOD(Int64(11)).AS("mod2"),
		AllTypes.SmallInt.POW(AllTypes.SmallInt.DIV(Int8(3))).AS("pow1"),
		AllTypes.SmallInt.POW(Int8(6)).AS("pow2"),

		AllTypes.SmallInt.BIT_AND(AllTypes.SmallInt).AS("bit_and1"),
		AllTypes.SmallInt.BIT_AND(AllTypes.SmallInt).AS("bit_and2"),
		AllTypes.SmallInt.BIT_OR(AllTypes.SmallInt).AS("bit or 1"),
		AllTypes.SmallInt.BIT_OR(Int(22)).AS("bit or 2"),
		AllTypes.SmallInt.BIT_XOR(AllTypes.SmallInt).AS("bit xor 1"),
		AllTypes.SmallInt.BIT_XOR(Int(11)).AS("bit xor 2"),

		BIT_NOT(Int(-1).MUL(AllTypes.SmallInt)).AS("bit_not_1"),
		BIT_NOT(Int(-11)).AS("bit_not_2"),

		AllTypes.SmallInt.BIT_SHIFT_LEFT(AllTypes.SmallInt.DIV(Int8(2))).AS("bit shift left 1"),
		AllTypes.SmallInt.BIT_SHIFT_LEFT(Int(4)).AS("bit shift left 2"),
		AllTypes.SmallInt.BIT_SHIFT_RIGHT(AllTypes.SmallInt.DIV(Int(5))).AS("bit shift right 1"),
		AllTypes.SmallInt.BIT_SHIFT_RIGHT(Int(1)).AS("bit shift right 2"),

		ABSi(AllTypes.BigInt).AS("abs"),
		SQRT(ABSi(AllTypes.BigInt)).AS("sqrt"),
		CBRT(ABSi(AllTypes.BigInt)).AS("cbrt"),
	).LIMIT(2)

	// fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, `
SELECT all_types.big_int AS "all_types.big_int",
     all_types.big_int_ptr AS "all_types.big_int_ptr",
     all_types.small_int AS "all_types.small_int",
     all_types.small_int_ptr AS "all_types.small_int_ptr",
     (all_types.big_int = all_types.big_int) AS "eq1",
     (all_types.big_int = $1::bigint) AS "eq2",
     (all_types.big_int != all_types.big_int_ptr) AS "neq1",
     (all_types.big_int != $2::bigint) AS "neq2",
     (all_types.big_int IS DISTINCT FROM all_types.big_int) AS "distinct1",
     (all_types.big_int IS DISTINCT FROM $3::integer) AS "distinct2",
     (all_types.big_int IS NOT DISTINCT FROM all_types.big_int) AS "not distinct1",
     (all_types.big_int IS NOT DISTINCT FROM $4::integer) AS "not distinct2",
     (all_types.integer BETWEEN $5 AND $6) AS "between",
     (all_types.integer NOT BETWEEN $7 AND $8) AS "not_between",
     (all_types.big_int < all_types.big_int_ptr) AS "lt1",
     (all_types.big_int < $9::smallint) AS "lt2",
     (all_types.big_int <= all_types.big_int_ptr) AS "lte1",
     (all_types.big_int <= $10::integer) AS "lte2",
     (all_types.big_int > all_types.big_int_ptr) AS "gt1",
     (all_types.big_int > $11::bigint) AS "gt2",
     (all_types.big_int >= all_types.big_int_ptr) AS "gte1",
     (all_types.big_int >= $12::bigint) AS "gte2",
     (all_types.big_int + all_types.big_int) AS "add1",
     (all_types.big_int + $13) AS "add2",
     (all_types.big_int - all_types.big_int) AS "sub1",
     (all_types.big_int - $14::smallint) AS "sub2",
     (all_types.big_int * all_types.big_int) AS "mul1",
     (all_types.big_int * $15::smallint) AS "mul2",
     (all_types.big_int / all_types.big_int) AS "div1",
     (all_types.big_int / $16::integer) AS "div2",
     (all_types.big_int % all_types.big_int) AS "mod1",
     (all_types.big_int % $17::bigint) AS "mod2",
     POW(all_types.small_int, all_types.small_int / $18::smallint) AS "pow1",
     POW(all_types.small_int, $19::smallint) AS "pow2",
     (all_types.small_int & all_types.small_int) AS "bit_and1",
     (all_types.small_int & all_types.small_int) AS "bit_and2",
     (all_types.small_int | all_types.small_int) AS "bit or 1",
     (all_types.small_int | $20) AS "bit or 2",
     (all_types.small_int # all_types.small_int) AS "bit xor 1",
     (all_types.small_int # $21) AS "bit xor 2",
     (~ ($22 * all_types.small_int)) AS "bit_not_1",
     (~ -11) AS "bit_not_2",
     (all_types.small_int << (all_types.small_int / $23::smallint)) AS "bit shift left 1",
     (all_types.small_int << $24) AS "bit shift left 2",
     (all_types.small_int >> (all_types.small_int / $25)) AS "bit shift right 1",
     (all_types.small_int >> $26) AS "bit shift right 2",
     ABS(all_types.big_int) AS "abs",
     SQRT(ABS(all_types.big_int)) AS "sqrt",
     CBRT(ABS(all_types.big_int)) AS "cbrt"
FROM test_sample.all_types
LIMIT $27;
`)

	var dest []struct {
		common.AllTypesIntegerExpResult `alias:"."`
	}

	err := query.Query(db, &dest)

	require.NoError(t, err)

	//testutils.SaveJSONFile(dest, "./testdata/results/common/int_operators.json")
	//testutils.PrintJson(dest)
	testutils.AssertJSONFile(t, dest, "./testdata/results/common/int_operators.json")
}

func TestTimeExpression(t *testing.T) {
	skipForCockroachDB(t)

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
		AllTypes.Time.BETWEEN(Time(11, 0, 30, 100), TimeT(time.Now())),
		AllTypes.Time.NOT_BETWEEN(AllTypes.TimePtr, AllTypes.Time.ADD(INTERVAL(2, HOUR))),

		AllTypes.Date.ADD(INTERVAL(1, HOUR)),
		AllTypes.Date.SUB(INTERVAL(1, MINUTE)),
		AllTypes.Time.ADD(INTERVAL(1, HOUR)),
		AllTypes.Time.SUB(INTERVAL(1, MINUTE)),
		AllTypes.Timez.ADD(INTERVAL(1, HOUR)),
		AllTypes.Timez.SUB(INTERVAL(1, MINUTE)),
		AllTypes.Timez.BETWEEN(TimezT(time.Now()), AllTypes.TimezPtr),
		AllTypes.Timez.NOT_BETWEEN(AllTypes.Timez, TimezT(time.Now())),
		AllTypes.Timestamp.ADD(INTERVAL(1, HOUR)),
		AllTypes.Timestamp.SUB(INTERVAL(1, MINUTE)),
		AllTypes.Timestamp.BETWEEN(AllTypes.TimestampPtr, TimestampT(time.Now())),
		AllTypes.Timestamp.NOT_BETWEEN(TimestampT(time.Now()), AllTypes.TimestampPtr),
		AllTypes.Timestampz.ADD(INTERVAL(1, HOUR)),
		AllTypes.Timestampz.SUB(INTERVAL(1, MINUTE)),
		AllTypes.Timestamp.BETWEEN(AllTypes.TimestampPtr, TimestampT(time.Now())),
		AllTypes.Timestamp.NOT_BETWEEN(AllTypes.TimestampPtr, TimestampT(time.Now())),

		AllTypes.Date.SUB(CAST(String("04:05:06")).AS_INTERVAL()),
		AllTypes.Date.BETWEEN(Date(2000, 2, 2), DateT(time.Now())),
		AllTypes.Date.NOT_BETWEEN(AllTypes.DatePtr, DateT(time.Now().Add(20*time.Hour))),

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

	// fmt.Println(query.DebugSql())

	var dest []struct{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestTimeScan(t *testing.T) {
	loc, err := time.LoadLocation("Japan")
	require.NoError(t, err)

	timeT := time.Date(3, 3, 3, 11, 22, 33, 0, time.UTC)
	timeWithNanoSeconds := time.Date(3, 3, 3, 1, 2, 3, 1000, time.UTC)

	timez := time.Date(3, 3, 3, 7, 8, 9, 0, time.UTC)
	timezWithNanoSeconds := time.Date(3, 3, 3, 4, 5, 6, 1000, loc)

	// '1999-01-08 04:05:06'
	timestamp := time.Date(1999, 01, 8, 4, 5, 6, 0, time.UTC)
	timestampWithNanoSeconds := time.Date(3, 3, 3, 8, 9, 10, 1000, time.UTC)

	timestampz := time.Date(2003, 10, 3, 9, 10, 11, 0, loc)
	timestampzWithNanoSeconds := time.Date(3, 3, 3, 8, 9, 10, 1000, loc)

	date := time.Date(2010, 2, 3, 0, 0, 0, 0, time.UTC)

	stmt := SELECT(
		TimeT(timeT).AS("time"),
		TimeT(timeWithNanoSeconds).AS("timeWithNanoSeconds"),
		TimezT(timez).AS("timez"),
		TimezT(timezWithNanoSeconds).AS("timezWithNanoSeconds"),
		Timestamp(1999, 01, 8, 4, 5, 6).AS("timestamp"),
		TimestampT(timestampWithNanoSeconds).AS("timestampWithNanoSeconds"),
		TimestampzT(timestampz).AS("timestampz"),
		TimestampzT(timestampzWithNanoSeconds).AS("timestampzWithNanoSeconds"),
		DateT(date).AS("date"),

		TimeT(timeT).ADD(INTERVAL(2, HOUR)).AS("timeExpression"),

		SELECT_JSON_OBJ(
			TimeT(timeT).AS("time"),
			TimeT(timeWithNanoSeconds).AS("timeWithNanoSeconds"),
			TimezT(timez).AS("timez"),
			TimezT(timezWithNanoSeconds).AS("timezWithNanoSeconds"),
			TimestampT(timestamp).AS("timestamp"),
			TimestampT(timestampWithNanoSeconds).AS("timestampWithNanoSeconds"),
			TimestampzT(timestampz).AS("timestampz"),
			TimestampzT(timestampzWithNanoSeconds).AS("timestampzWithNanoSeconds"),
			DateT(date).AS("date"),

			TimeT(timeT).ADD(INTERVAL(2, HOUR)).AS("timeExpression"),
		).AS("json"),
	)

	testutils.AssertStatementSql(t, stmt, `
SELECT $1::time without time zone AS "time",
     $2::time without time zone AS "timeWithNanoSeconds",
     $3::time with time zone AS "timez",
     $4::time with time zone AS "timezWithNanoSeconds",
     $5::timestamp without time zone AS "timestamp",
     $6::timestamp without time zone AS "timestampWithNanoSeconds",
     $7::timestamp with time zone AS "timestampz",
     $8::timestamp with time zone AS "timestampzWithNanoSeconds",
     $9::date AS "date",
     ($10::time without time zone + INTERVAL '2 HOUR') AS "timeExpression",
     (
          SELECT row_to_json(json_records) AS "json_json"
          FROM (
                    SELECT '0000-01-01T' || to_char('2000-10-10'::date + $11::time without time zone, 'HH24:MI:SS.USZ') AS "time",
                         '0000-01-01T' || to_char('2000-10-10'::date + $12::time without time zone, 'HH24:MI:SS.USZ') AS "timeWithNanoSeconds",
                         '0000-01-01T' || to_char('2000-10-10'::date + $13::time with time zone, 'HH24:MI:SS.USTZH:TZM') AS "timez",
                         '0000-01-01T' || to_char('2000-10-10'::date + $14::time with time zone, 'HH24:MI:SS.USTZH:TZM') AS "timezWithNanoSeconds",
                         to_char($15::timestamp without time zone, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "timestamp",
                         to_char($16::timestamp without time zone, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "timestampWithNanoSeconds",
                         $17::timestamp with time zone AS "timestampz",
                         $18::timestamp with time zone AS "timestampzWithNanoSeconds",
                         to_char($19::date::timestamp, 'YYYY-MM-DD') || 'T00:00:00Z' AS "date",
                         '0000-01-01T' || to_char('2000-10-10'::date + ($20::time without time zone + INTERVAL '2 HOUR'), 'HH24:MI:SS.USZ') AS "timeExpression"
               ) AS json_records
     ) AS "json";
`)

	var dest struct {
		Time                      time.Time
		TimeWithNanoSeconds       time.Time
		Timez                     time.Time
		TimezWithNanoSeconds      time.Time
		Timestamp                 time.Time
		TimestampWithNanoSeconds  time.Time
		Timestampz                time.Time
		TimestampzWithNanoSeconds time.Time
		Date                      time.Time

		TimeExpression time.Time

		Json struct {
			Time                      time.Time
			TimeWithNanoSeconds       time.Time
			Timez                     time.Time
			TimezWithNanoSeconds      time.Time
			Timestamp                 time.Time
			TimestampWithNanoSeconds  time.Time
			Timestampz                time.Time
			TimestampzWithNanoSeconds time.Time
			Date                      time.Time

			TimeExpression time.Time
		} `json_column:"json"`
	}

	err = stmt.Query(db, &dest)
	require.NoError(t, err)

	ensureTimezEqual(t, timeT.Add(2*time.Hour), dest.TimeExpression, loc)
	ensureTimezEqual(t, timeT.Add(2*time.Hour), dest.Json.TimeExpression, loc)

	ensureTimezEqual(t, timeT, dest.Time, loc)
	ensureTimezEqual(t, timeT, dest.Json.Time, loc)
	ensureTimezEqual(t, timeWithNanoSeconds, dest.TimeWithNanoSeconds, loc)
	ensureTimezEqual(t, timeWithNanoSeconds, dest.Json.TimeWithNanoSeconds, loc)

	ensureTimezEqual(t, timez, dest.Timez, loc)
	ensureTimezEqual(t, timez, dest.Json.Timez, loc)
	ensureTimezEqual(t, timezWithNanoSeconds, dest.TimezWithNanoSeconds, loc)
	ensureTimezEqual(t, timezWithNanoSeconds, dest.Json.TimezWithNanoSeconds, loc)

	ensureTimezEqual(t, timestamp, dest.Timestamp, loc)
	ensureTimezEqual(t, timestamp, dest.Json.Timestamp, loc)
	ensureTimezEqual(t, timestampWithNanoSeconds, dest.TimestampWithNanoSeconds, loc)
	ensureTimezEqual(t, timestampWithNanoSeconds, dest.Json.TimestampWithNanoSeconds, loc)

	ensureTimezEqual(t, timestampz, dest.Timestampz, loc)
	ensureTimezEqual(t, timestampz, dest.Json.Timestampz, loc)
	ensureTimezEqual(t, timestampzWithNanoSeconds, dest.TimestampzWithNanoSeconds, loc)
	ensureTimezEqual(t, timestampzWithNanoSeconds, dest.Json.TimestampzWithNanoSeconds, loc)

	ensureTimezEqual(t, date, dest.Date, loc)
	ensureTimezEqual(t, date, dest.Json.Date, loc)

	t.Run("json only", func(t *testing.T) {
		stmtJson := SELECT_JSON_OBJ(
			TimeT(timeT).AS("time"),
			TimeT(timeWithNanoSeconds).AS("timeWithNanoSeconds"),

			TimezT(timez).AS("timez"),
			TimezT(timezWithNanoSeconds).AS("timezWithNanoSeconds"),

			Timestamp(1999, 01, 8, 4, 5, 6).AS("timestamp"),
			TimestampT(timestampWithNanoSeconds).AS("timestampWithNanoSeconds"),

			TimestampzT(timestampz).AS("timestampz"),
			TimestampzT(timestampzWithNanoSeconds).AS("timestampzWithNanoSeconds"),

			DateT(date).AS("date"),
		)

		var jsonDest struct {
			Time                time.Time
			TimeWithNanoSeconds time.Time

			Timez                time.Time
			TimezWithNanoSeconds time.Time

			Timestamp                time.Time
			TimestampWithNanoSeconds time.Time

			Timestampz                time.Time
			TimestampzWithNanoSeconds time.Time

			Date time.Time
		}

		err := stmtJson.QueryContext(ctx, db, &jsonDest)
		require.NoError(t, err)
	})
}

func ensureTimezEqual(t *testing.T, time1, time2 time.Time, loc *time.Location) {
	time1Loc := time1.In(loc)
	time2Loc := time2.In(loc)

	require.Equal(t, time1Loc.Hour(), time2Loc.Hour())
	require.Equal(t, time1Loc.Minute(), time2Loc.Minute())
	require.Equal(t, time1Loc.Second(), time2Loc.Second())
	require.Equal(t, toMicroSeconds(time1Loc.Nanosecond()), toMicroSeconds(time2Loc.Nanosecond()))
}

func toMicroSeconds(nanoseconds int) int {
	return nanoseconds / 1000
}

func TestIntervalSetFunctionality(t *testing.T) {

	t.Run("updateQueryIntervalTest", func(t *testing.T) {

		expectedQuery := `
UPDATE test_sample.employee
SET pto_accrual = INTERVAL '3 HOUR'
WHERE employee.employee_id = $1
RETURNING employee.employee_id AS "employee.employee_id",
          employee.first_name AS "employee.first_name",
          employee.last_name AS "employee.last_name",
          employee.employment_date AS "employee.employment_date",
          employee.manager_id AS "employee.manager_id",
          employee.pto_accrual AS "employee.pto_accrual";
`
		testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
			var windy model.Employee
			windy.PtoAccrual = ptr.Of("3h")
			stmt := Employee.UPDATE(Employee.PtoAccrual).SET(
				Employee.PtoAccrual.SET(INTERVAL(3, HOUR)),
			).WHERE(Employee.EmployeeID.EQ(Int(1))).RETURNING(Employee.AllColumns)

			testutils.AssertStatementSql(t, stmt, expectedQuery)
			err := stmt.Query(tx, &windy)
			assert.Nil(t, err)
			assert.Equal(t, *windy.PtoAccrual, "03:00:00")

		})
	})

	t.Run("upsertQueryIntervalTest", func(t *testing.T) {
		expectedQuery := `
INSERT INTO test_sample.employee (employee_id, first_name, last_name, employment_date, manager_id, pto_accrual)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (employee_id) DO UPDATE
       SET pto_accrual = excluded.pto_accrual
RETURNING employee.employee_id AS "employee.employee_id",
          employee.first_name AS "employee.first_name",
          employee.last_name AS "employee.last_name",
          employee.employment_date AS "employee.employment_date",
          employee.manager_id AS "employee.manager_id",
          employee.pto_accrual AS "employee.pto_accrual";
`
		testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
			var employee model.Employee
			employee.PtoAccrual = ptr.Of("5h")
			stmt := Employee.INSERT(Employee.AllColumns).
				MODEL(employee).
				ON_CONFLICT(Employee.EmployeeID).
				DO_UPDATE(SET(
					Employee.PtoAccrual.SET(Employee.EXCLUDED.PtoAccrual),
				)).RETURNING(Employee.AllColumns)

			testutils.AssertStatementSql(t, stmt, expectedQuery)
			err := stmt.Query(tx, &employee)
			assert.Nil(t, err)
			assert.Equal(t, *employee.PtoAccrual, "05:00:00")

		})
	})
}

func TestInterval(t *testing.T) {
	skipForCockroachDB(t)

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
		AllTypes.Interval.BETWEEN(INTERVAL(1, HOUR), INTERVAL(2, HOUR)),
		AllTypes.Interval.NOT_BETWEEN(AllTypes.IntervalPtr, INTERVALd(30*time.Second)),
		AllTypes.Interval.ADD(AllTypes.IntervalPtr).EQ(INTERVALd(17*time.Second)),
		AllTypes.Interval.SUB(AllTypes.IntervalPtr).EQ(INTERVAL(100, MICROSECOND)),
		AllTypes.IntervalPtr.MUL(Int(11)).EQ(AllTypes.Interval),
		AllTypes.IntervalPtr.DIV(Float(22.222)).EQ(AllTypes.IntervalPtr),
	).FROM(AllTypes)

	fmt.Println(stmt.Sql())

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT INTERVAL '1 YEAR',
     INTERVAL '1 MONTH',
     INTERVAL '1 WEEK',
     INTERVAL '1 DAY',
     INTERVAL '1 HOUR',
     INTERVAL '1 MINUTE',
     INTERVAL '1 SECOND',
     INTERVAL '1 MILLISECOND',
     INTERVAL '1 MICROSECOND',
     INTERVAL '1 DECADE',
     INTERVAL '1 CENTURY',
     INTERVAL '1 MILLENNIUM',
     INTERVAL '1 YEAR 10 MONTH',
     INTERVAL '1 YEAR 10 MONTH 20 DAY',
     INTERVAL '1 YEAR 10 MONTH 20 DAY 3 HOUR',
     INTERVAL '1 YEAR' IS NOT NULL,
     INTERVAL '1 YEAR' AS "one year",
     INTERVAL '0 MICROSECOND',
     INTERVAL '1 MICROSECOND',
     INTERVAL '1000 MICROSECOND',
     INTERVAL '1 SECOND',
     INTERVAL '1 MINUTE',
     INTERVAL '1 HOUR',
     INTERVAL '1 DAY',
     INTERVAL '1 DAY 2 HOUR 3 MINUTE 4 SECOND 5 MICROSECOND',
     (all_types.interval = INTERVAL '2 HOUR 20 MINUTE') = TRUE::boolean,
     (all_types.interval_ptr != INTERVAL '2 HOUR 20 MINUTE') = FALSE::boolean,
     (all_types.interval IS DISTINCT FROM INTERVAL '2 HOUR 20 MINUTE') = all_types.boolean,
     (all_types.interval_ptr IS NOT DISTINCT FROM INTERVAL '10 MICROSECOND') = all_types.boolean,
     (all_types.interval < all_types.interval_ptr) = all_types.boolean_ptr,
     (all_types.interval <= all_types.interval_ptr) = all_types.boolean_ptr,
     (all_types.interval > all_types.interval_ptr) = all_types.boolean_ptr,
     (all_types.interval >= all_types.interval_ptr) = all_types.boolean_ptr,
     all_types.interval BETWEEN INTERVAL '1 HOUR' AND INTERVAL '2 HOUR',
     all_types.interval NOT BETWEEN all_types.interval_ptr AND INTERVAL '30 SECOND',
     (all_types.interval + all_types.interval_ptr) = INTERVAL '17 SECOND',
     (all_types.interval - all_types.interval_ptr) = INTERVAL '100 MICROSECOND',
     (all_types.interval_ptr * 11) = all_types.interval,
     (all_types.interval_ptr / 22.222) = all_types.interval_ptr
FROM test_sample.all_types;
`)

	err := stmt.Query(db, &struct{}{})
	require.NoError(t, err)
	requireLogged(t, stmt)
}

func TestTimeEXTRACT(t *testing.T) {
	stmt := SELECT(
		EXTRACT(CENTURY, AllTypes.Timestampz),
		EXTRACT(DAY, AllTypes.Timestamp),
		EXTRACT(DECADE, AllTypes.Date),
		EXTRACT(DOW, AllTypes.TimestampzPtr),
		EXTRACT(DOY, DateT(time.Now())),
		EXTRACT(EPOCH, TimestampT(time.Now())),
		EXTRACT(HOUR, AllTypes.Time.ADD(INTERVAL(1, HOUR))),
		EXTRACT(ISODOW, AllTypes.Timestampz),
		EXTRACT(ISOYEAR, AllTypes.Timestampz),
		EXTRACT(JULIAN, AllTypes.Timestampz).EQ(Float(3456.123)),
		EXTRACT(MICROSECOND, AllTypes.Timestampz),
		EXTRACT(MILLENNIUM, AllTypes.Timestampz),
		EXTRACT(MILLISECOND, AllTypes.Timez),
		EXTRACT(MINUTE, INTERVAL(1, HOUR, 2, MINUTE)),
		EXTRACT(MONTH, AllTypes.Timestampz),
		EXTRACT(QUARTER, AllTypes.Timestampz),
		EXTRACT(SECOND, AllTypes.Timestampz),
		EXTRACT(TIMEZONE, AllTypes.Timestampz),
		EXTRACT(TIMEZONE_HOUR, AllTypes.Timestampz),
		EXTRACT(TIMEZONE_MINUTE, AllTypes.Timestampz),
		EXTRACT(WEEK, AllTypes.Timestampz),
		EXTRACT(YEAR, AllTypes.Timestampz),
	).FROM(
		AllTypes,
	)

	// fmt.Println(stmt.Sql())

	testutils.AssertStatementSql(t, stmt, `
SELECT EXTRACT(CENTURY FROM all_types.timestampz),
     EXTRACT(DAY FROM all_types.timestamp),
     EXTRACT(DECADE FROM all_types.date),
     EXTRACT(DOW FROM all_types.timestampz_ptr),
     EXTRACT(DOY FROM $1::date),
     EXTRACT(EPOCH FROM $2::timestamp without time zone),
     EXTRACT(HOUR FROM all_types.time + INTERVAL '1 HOUR'),
     EXTRACT(ISODOW FROM all_types.timestampz),
     EXTRACT(ISOYEAR FROM all_types.timestampz),
     EXTRACT(JULIAN FROM all_types.timestampz) = $3,
     EXTRACT(MICROSECOND FROM all_types.timestampz),
     EXTRACT(MILLENNIUM FROM all_types.timestampz),
     EXTRACT(MILLISECOND FROM all_types.timez),
     EXTRACT(MINUTE FROM INTERVAL '1 HOUR 2 MINUTE'),
     EXTRACT(MONTH FROM all_types.timestampz),
     EXTRACT(QUARTER FROM all_types.timestampz),
     EXTRACT(SECOND FROM all_types.timestampz),
     EXTRACT(TIMEZONE FROM all_types.timestampz),
     EXTRACT(TIMEZONE_HOUR FROM all_types.timestampz),
     EXTRACT(TIMEZONE_MINUTE FROM all_types.timestampz),
     EXTRACT(WEEK FROM all_types.timestampz),
     EXTRACT(YEAR FROM all_types.timestampz)
FROM test_sample.all_types;
`)

	err := stmt.Query(db, &struct{}{})
	require.NoError(t, err)
}

func TestRowExpression(t *testing.T) {
	now := time.Now()
	nowAddHour := time.Now().Add(time.Hour)

	stmt := SELECT(
		ROW(Int32(1), Real(11.22), Text("john")).AS("row"),
		WRAP(Int64(1), Double(11.22), VarChar(10)("john")).AS("wrap"),

		ROW(Bool(false), DateT(now)).EQ(ROW(Bool(true), DateT(now))),
		WRAP(Bool(false), DateT(now)).NOT_EQ(WRAP(Bool(true), DateT(now))),

		ROW(TimeT(nowAddHour)).IS_DISTINCT_FROM(RowExp(Raw("row(NOW()::time)"))),
		ROW().IS_NOT_DISTINCT_FROM(ROW()),

		ROW(TimestampT(now), TimestampzT(nowAddHour)).GT(WRAP(TimestampT(now), TimestampzT(now))),
		ROW(TimestampzT(nowAddHour)).GT_EQ(ROW(TimestampzT(now))),
		WRAP(TimestampT(now), TimestampzT(nowAddHour)).LT(ROW(TimestampT(now), TimestampzT(now))),
		ROW(TimestampzT(nowAddHour)).LT_EQ(ROW(TimestampzT(now))),
	)

	//fmt.Println(stmt.Sql())
	//fmt.Println(stmt.DebugSql())

	testutils.AssertStatementSql(t, stmt, `
SELECT ROW($1::integer, $2::real, $3::text) AS "row",
     ($4::bigint, $5::double precision, $6::varchar(10)) AS "wrap",
     ROW($7::boolean, $8::date) = ROW($9::boolean, $10::date),
     ($11::boolean, $12::date) != ($13::boolean, $14::date),
     ROW($15::time without time zone) IS DISTINCT FROM (row(NOW()::time)),
     ROW() IS NOT DISTINCT FROM ROW(),
     ROW($16::timestamp without time zone, $17::timestamp with time zone) > ($18::timestamp without time zone, $19::timestamp with time zone),
     ROW($20::timestamp with time zone) >= ROW($21::timestamp with time zone),
     ($22::timestamp without time zone, $23::timestamp with time zone) < ROW($24::timestamp without time zone, $25::timestamp with time zone),
     ROW($26::timestamp with time zone) <= ROW($27::timestamp with time zone);
`)

	err := stmt.Query(db, &struct{}{})
	require.NoError(t, err)
}

func TestAllTypesSubQueryFrom(t *testing.T) {
	subQuery := SELECT(
		AllTypes.Boolean,
		AllTypes.Integer,
		AllTypes.DoublePrecision,
		AllTypes.Text,
		AllTypes.Date,
		AllTypes.Time,
		AllTypes.Timez,
		AllTypes.Timestamp,
		AllTypes.Timestampz,
		AllTypes.Interval,
		AllTypes.Bytea,
	).FROM(
		AllTypes,
	).AsTable("subQuery")

	stmt := SELECT(
		AllTypes.Boolean.From(subQuery),
		AllTypes.Integer.From(subQuery),
		AllTypes.DoublePrecision.From(subQuery),
		AllTypes.Text.From(subQuery),
		AllTypes.Date.From(subQuery),
		AllTypes.Time.From(subQuery),
		AllTypes.Timez.From(subQuery),
		AllTypes.Timestamp.From(subQuery),
		AllTypes.Timestampz.From(subQuery),
		AllTypes.Interval.From(subQuery),
		AllTypes.Bytea.From(subQuery),
	).FROM(
		subQuery,
	)

	testutils.AssertStatementSql(t, stmt, `
SELECT "subQuery"."all_types.boolean" AS "all_types.boolean",
     "subQuery"."all_types.integer" AS "all_types.integer",
     "subQuery"."all_types.double_precision" AS "all_types.double_precision",
     "subQuery"."all_types.text" AS "all_types.text",
     "subQuery"."all_types.date" AS "all_types.date",
     "subQuery"."all_types.time" AS "all_types.time",
     "subQuery"."all_types.timez" AS "all_types.timez",
     "subQuery"."all_types.timestamp" AS "all_types.timestamp",
     "subQuery"."all_types.timestampz" AS "all_types.timestampz",
     "subQuery"."all_types.interval" AS "all_types.interval",
     "subQuery"."all_types.bytea" AS "all_types.bytea"
FROM (
          SELECT all_types.boolean AS "all_types.boolean",
               all_types.integer AS "all_types.integer",
               all_types.double_precision AS "all_types.double_precision",
               all_types.text AS "all_types.text",
               all_types.date AS "all_types.date",
               all_types.time AS "all_types.time",
               all_types.timez AS "all_types.timez",
               all_types.timestamp AS "all_types.timestamp",
               all_types.timestampz AS "all_types.timestampz",
               all_types.interval AS "all_types.interval",
               all_types.bytea AS "all_types.bytea"
          FROM test_sample.all_types
     ) AS "subQuery";
`)

	var dest []model.AllTypes

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	t.Run("using SELECT_JSON", func(t *testing.T) {
		stmtJson := SELECT_JSON_ARR(
			AllTypes.Boolean.From(subQuery),
			AllTypes.Integer.From(subQuery),
			AllTypes.DoublePrecision.From(subQuery),
			AllTypes.Text.From(subQuery),
			AllTypes.Date.From(subQuery),
			AllTypes.Time.From(subQuery),
			AllTypes.Timez.From(subQuery),
			AllTypes.Timestamp.From(subQuery),
			AllTypes.Timestampz.From(subQuery),
			AllTypes.Interval.From(subQuery),
			AllTypes.Bytea.From(subQuery),
		).FROM(
			subQuery,
		)

		testutils.AssertDebugStatementSql(t, stmtJson, `
SELECT json_agg(row_to_json(records)) AS "json"
FROM (
          SELECT "subQuery"."all_types.boolean" AS "boolean",
               "subQuery"."all_types.integer" AS "integer",
               "subQuery"."all_types.double_precision" AS "doublePrecision",
               "subQuery"."all_types.text" AS "text",
               to_char("subQuery"."all_types.date"::timestamp, 'YYYY-MM-DD') || 'T00:00:00Z' AS "date",
               '0000-01-01T' || to_char('2000-10-10'::date + "subQuery"."all_types.time", 'HH24:MI:SS.USZ') AS "time",
               '0000-01-01T' || to_char('2000-10-10'::date + "subQuery"."all_types.timez", 'HH24:MI:SS.USTZH:TZM') AS "timez",
               to_char("subQuery"."all_types.timestamp", 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS "timestamp",
               "subQuery"."all_types.timestampz" AS "timestampz",
               "subQuery"."all_types.interval" AS "interval",
               ENCODE("subQuery"."all_types.bytea", 'base64') AS "bytea"
          FROM (
                    SELECT all_types.boolean AS "all_types.boolean",
                         all_types.integer AS "all_types.integer",
                         all_types.double_precision AS "all_types.double_precision",
                         all_types.text AS "all_types.text",
                         all_types.date AS "all_types.date",
                         all_types.time AS "all_types.time",
                         all_types.timez AS "all_types.timez",
                         all_types.timestamp AS "all_types.timestamp",
                         all_types.timestampz AS "all_types.timestampz",
                         all_types.interval AS "all_types.interval",
                         all_types.bytea AS "all_types.bytea"
                    FROM test_sample.all_types
               ) AS "subQuery"
     ) AS records;
`)

		var destJson []model.AllTypes

		err := stmtJson.QueryContext(ctx, db, &destJson)
		require.NoError(t, err)

		t.Run("using AllColumns()", func(t *testing.T) {
			stmtJsonAllColumns := SELECT_JSON_ARR(
				subQuery.AllColumns(),
			).FROM(
				subQuery,
			)

			require.Equal(t, stmtJson.DebugSql(), stmtJsonAllColumns.DebugSql())
		})

		// fix timezone before comparisons
		minus8 := time.FixedZone("UTC", -8*60*60)
		destJson[0].Timez = *toTZ(&destJson[0].Timez, minus8)
		destJson[1].Timez = *toTZ(&destJson[1].Timez, minus8)

		destJson[0].Timestampz = *toTZ(&destJson[0].Timestampz, time.UTC)
		destJson[1].Timestampz = *toTZ(&destJson[1].Timestampz, time.UTC)

		dest[0].Timestampz = *toTZ(&dest[0].Timestampz, time.UTC)
		dest[1].Timestampz = *toTZ(&dest[1].Timestampz, time.UTC)

		testutils.AssertJsonEqual(t, dest, destJson)
	})
}

func TestAllTypesUpdateSet(t *testing.T) {

	stmt := AllTypes.UPDATE().
		SET(
			AllTypes.Boolean.SET(Bool(false)),
			AllTypes.Integer.SET(Int(2)),
			AllTypes.DoublePrecision.SET(Float(2.22)),
			AllTypes.Text.SET(Text("some text")),
			AllTypes.Date.SET(DateT(time.Now())),
			AllTypes.Time.SET(TimeT(time.Now())),
			AllTypes.Timez.SET(TimezT(time.Now())),
			AllTypes.Timestamp.SET(TimestampT(time.Now())),
			AllTypes.Interval.SET(INTERVAL(1, HOUR)),
			AllTypes.Bytea.SET(Bytea([]byte{11, 22, 33, 44})),
		).WHERE(Bool(true))

	testutils.AssertStatementSql(t, stmt, `
UPDATE test_sample.all_types
SET boolean = $1::boolean,
    integer = $2,
    double_precision = $3,
    text = $4::text,
    date = $5::date,
    time = $6::time without time zone,
    timez = $7::time with time zone,
    timestamp = $8::timestamp without time zone,
    interval = INTERVAL '1 HOUR',
    bytea = $9::bytea
WHERE $10::boolean;
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		_, err := stmt.Exec(tx)
		require.NoError(t, err)
	})
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
		Date       time.Time
		Time       time.Time
		Timez      time.Time
		Timestamp  time.Time
		Timestampz time.Time
	}

	err = query.Query(db, &dest)

	require.NoError(t, err)

	// pq driver will return time with time zone in local timezone,
	// while pgx driver will return time in UTC time zone
	dest.Timez = dest.Timez.UTC()
	dest.Timestampz = dest.Timestampz.UTC()

	if sourceIsCockroachDB() {
		return // rounding differences
	}

	testutils.AssertJSON(t, dest, `
{
	"Date": "2009-11-17T00:00:00Z",
	"Time": "0000-01-01T20:34:58.651387Z",
	"Timez": "0000-01-01T19:34:58.651387Z",
	"Timestamp": "2009-11-17T20:34:58.651387Z",
	"Timestampz": "2009-11-17T19:34:58.651387Z"
}
`)
	requireLogged(t, query)
}

func TestJsonLiteral(t *testing.T) {
	stmt := AllTypes.UPDATE().
		SET(AllTypes.JSON.SET(Json(`{"firstName": "John", "lastName": "Doe"}`))).
		WHERE(AllTypes.SmallInt.EQ(Int(14))).
		RETURNING(AllTypes.JSON)

	testutils.AssertDebugStatementSql(t, stmt, `
UPDATE test_sample.all_types
SET json = '{"firstName": "John", "lastName": "Doe"}'::json
WHERE all_types.small_int = 14
RETURNING all_types.json AS "all_types.json";
`)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var res model.AllTypes

		err := stmt.Query(tx, &res)
		require.NoError(t, err)
		require.Equal(t, res.JSON, `{"firstName": "John", "lastName": "Doe"}`)
	})
}

var moodSad = model.Mood_Sad

var allTypesRow0 = model.AllTypes{
	SmallIntPtr:        ptr.Of(int16(14)),
	SmallInt:           14,
	IntegerPtr:         ptr.Of(int32(300)),
	Integer:            300,
	BigIntPtr:          ptr.Of(int64(50000)),
	BigInt:             5000,
	DecimalPtr:         ptr.Of(1.11),
	Decimal:            1.11,
	NumericPtr:         ptr.Of(2.22),
	Numeric:            2.22,
	RealPtr:            ptr.Of(float32(5.55)),
	Real:               5.55,
	DoublePrecisionPtr: ptr.Of(11111111.22),
	DoublePrecision:    11111111.22,
	Smallserial:        1,
	Serial:             1,
	Bigserial:          1,
	//MoneyPtr: nil,
	//Money:
	VarCharPtr:           ptr.Of("ABBA"),
	VarChar:              "ABBA",
	CharPtr:              ptr.Of("JOHN                                                                            "),
	Char:                 "JOHN                                                                            ",
	TextPtr:              ptr.Of("Some text"),
	Text:                 "Some text",
	ByteaPtr:             ptr.Of([]byte("bytea")),
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
	IntervalPtr:          ptr.Of("3 days 04:05:06"),
	Interval:             "3 days 04:05:06",
	BooleanPtr:           ptr.Of(true),
	Boolean:              false,
	PointPtr:             ptr.Of("(2,3)"),
	BitPtr:               ptr.Of("101"),
	Bit:                  "101",
	BitVaryingPtr:        ptr.Of("101111"),
	BitVarying:           "101111",
	TsvectorPtr:          ptr.Of("'supernova':1"),
	Tsvector:             "'supernova':1",
	UUIDPtr:              testutils.UUIDPtr("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	UUID:                 uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	XMLPtr:               ptr.Of("<Sub>abc</Sub>"),
	XML:                  "<Sub>abc</Sub>",
	JSONPtr:              ptr.Of(`{"a": 1, "b": 3}`),
	JSON:                 `{"a": 1, "b": 3}`,
	JsonbPtr:             ptr.Of(`{"a": 1, "b": 3}`),
	Jsonb:                `{"a": 1, "b": 3}`,
	IntegerArrayPtr:      ptr.Of("{1,2,3}"),
	IntegerArray:         "{1,2,3}",
	TextArrayPtr:         ptr.Of("{breakfast,consulting}"),
	TextArray:            "{breakfast,consulting}",
	JsonbArray:           `{"{\"a\": 1, \"b\": 2}","{\"a\": 3, \"b\": 4}"}`,
	TextMultiDimArrayPtr: ptr.Of("{{meeting,lunch},{training,presentation}}"),
	TextMultiDimArray:    "{{meeting,lunch},{training,presentation}}",
	MoodPtr:              &moodSad,
	Mood:                 model.Mood_Happy,
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
	MoodPtr:              nil,
	Mood:                 model.Mood_Ok,
}

func TestAliasedDuplicateSliceSubType(t *testing.T) {

	children := Components.AS("children")
	childrenVulnerabilities := Vulnerabilities.AS("children_vulnerabilities")

	stmt := SELECT(
		Components.AllColumns,
		Vulnerabilities.AllColumns,
		children.AllColumns,
		childrenVulnerabilities.AllColumns,
	).FROM(
		Components.
			LEFT_JOIN(Vulnerabilities, Vulnerabilities.ComponentsID.EQ(Components.ID)).
			LEFT_JOIN(children, children.ParentID.EQ(Components.ID)).
			LEFT_JOIN(childrenVulnerabilities, childrenVulnerabilities.ComponentsID.EQ(children.ID)),
	).ORDER_BY(
		Components.ID,
		Vulnerabilities.ID,
		children.ID,
		childrenVulnerabilities.ID,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT components.id AS "components.id",
     components.parent_id AS "components.parent_id",
     vulnerabilities.id AS "vulnerabilities.id",
     vulnerabilities.components_id AS "vulnerabilities.components_id",
     children.id AS "children.id",
     children.parent_id AS "children.parent_id",
     children_vulnerabilities.id AS "children_vulnerabilities.id",
     children_vulnerabilities.components_id AS "children_vulnerabilities.components_id"
FROM test_sample.components
     LEFT JOIN test_sample.vulnerabilities ON (vulnerabilities.components_id = components.id)
     LEFT JOIN test_sample.components AS children ON (children.parent_id = components.id)
     LEFT JOIN test_sample.vulnerabilities AS children_vulnerabilities ON (children_vulnerabilities.components_id = children.id)
ORDER BY components.id, vulnerabilities.id, children.id, children_vulnerabilities.id;
`)

	var dest []struct {
		model.Components
		Vulnerabilities []model.Vulnerabilities
		Children        []struct {
			model.Components `alias:"children"`
			Vulnerabilities  []model.Vulnerabilities `alias:"children_vulnerabilities"`
		}
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	testutils.AssertJSON(t, dest, `
[
	{
		"ID": "component_00",
		"ParentID": null,
		"Vulnerabilities": [
			{
				"ID": "vulnerability_00",
				"ComponentsID": "component_00"
			},
			{
				"ID": "vulnerability_01",
				"ComponentsID": "component_00"
			},
			{
				"ID": "vulnerability_02",
				"ComponentsID": "component_00"
			},
			{
				"ID": "vulnerability_03",
				"ComponentsID": "component_00"
			}
		],
		"Children": [
			{
				"ID": "component_01",
				"ParentID": "component_00",
				"Vulnerabilities": [
					{
						"ID": "vulnerability_11",
						"ComponentsID": "component_01"
					},
					{
						"ID": "vulnerability_12",
						"ComponentsID": "component_01"
					}
				]
			},
			{
				"ID": "component_02",
				"ParentID": "component_00",
				"Vulnerabilities": [
					{
						"ID": "vulnerability_21",
						"ComponentsID": "component_02"
					}
				]
			}
		]
	},
	{
		"ID": "component_01",
		"ParentID": "component_00",
		"Vulnerabilities": [
			{
				"ID": "vulnerability_11",
				"ComponentsID": "component_01"
			},
			{
				"ID": "vulnerability_12",
				"ComponentsID": "component_01"
			}
		],
		"Children": null
	},
	{
		"ID": "component_02",
		"ParentID": "component_00",
		"Vulnerabilities": [
			{
				"ID": "vulnerability_21",
				"ComponentsID": "component_02"
			}
		],
		"Children": null
	}
]
`)
}
