package postgres

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
	"github.com/google/go-cmp/cmp"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
)

func TestArrayTableSelect(t *testing.T) {
	skipForCockroachDB(t)

	textArray := StringArray([]string{"a"})
	boolArray := BoolArray([]bool{true})
	int4Array := Int32Array([]int32{1, 2})
	int8Array := Int64Array([]int64{10, 11})

	query := SELECT(
		SampleArrays.AllColumns,
		SampleArrays.TextArray.EQ(SampleArrays.TextArray).AS("sample.text_eq"),
		SampleArrays.BoolArray.EQ(boolArray).AS("sample.bool_eq"),
		SampleArrays.TextArray.NOT_EQ(textArray).AS("sample.text_neq"),
		SampleArrays.Int4Array.LT(int4Array).IS_TRUE().AS("sample.int4_lt"),
		SampleArrays.Int8Array.LT_EQ(int8Array).IS_FALSE().AS("sample.int8_lteq"),
		SampleArrays.TextArray.GT(textArray).AS("sample.text_gt"),
		SampleArrays.Int4Array.GT_EQ(int4Array).AS("sample.bool_gteq"),
		Int32(22).EQ(ANY[IntegerExpression](SampleArrays.Int4Array)).AS("sample.int4_eq_any"),
		Int32(22).NOT_EQ(ANY[IntegerExpression](SampleArrays.Int4Array)).AS("sample.int4_neq_any"),
		Int32(22).EQ(ALL[IntegerExpression](SampleArrays.Int4Array)).AS("sample.int4_eq_all"),
		SampleArrays.Int8Array.CONTAINS(Int64Array([]int64{75364})).AS("sample.int8cont"),
		SampleArrays.Int8Array.IS_CONTAINED_BY(Int64Array([]int64{75364})).AS("sample.int8cont_by"),
		SampleArrays.Int4Array.OVERLAP(int4Array).AS("sample.int4_overlap"),
		SampleArrays.BoolArray.CONCAT(boolArray).AS("sample.bool_concat"),
		SampleArrays.TextArray.CONCAT_ELEMENT(String("z")).AS("sample.text_concat_el"),
		SampleArrays.TextArray.AT(Int32(1)).AS("sample.text_at"),
		ARRAY_APPEND[StringExpression](SampleArrays.TextArray, String("after")).AS("sample.text_append"),
		ARRAY_CAT[StringExpression](SampleArrays.TextArray, textArray).AS("sample.text_cat"),
		ARRAY_LENGTH[StringExpression](SampleArrays.TextArray, Int32(1)).AS("sample.text_length"),
		ARRAY_PREPEND[StringExpression](String("before"), SampleArrays.TextArray).AS("sample.text_prepend"),
	).FROM(
		SampleArrays,
	).WHERE(
		SampleArrays.BoolArray.CONTAINS(BoolArray([]bool{true})),
	)

	testutils.AssertStatementSql(t, query, `
SELECT sample_arrays.text_array AS "sample_arrays.text_array",
     sample_arrays.bool_array AS "sample_arrays.bool_array",
     sample_arrays.int4_array AS "sample_arrays.int4_array",
     sample_arrays.int8_array AS "sample_arrays.int8_array",
     (sample_arrays.text_array = sample_arrays.text_array) AS "sample.text_eq",
     (sample_arrays.bool_array = $1) AS "sample.bool_eq",
     (sample_arrays.text_array != $2) AS "sample.text_neq",
     (sample_arrays.int4_array < $3) IS TRUE AS "sample.int4_lt",
     (sample_arrays.int8_array <= $4) IS FALSE AS "sample.int8_lteq",
     (sample_arrays.text_array > $5) AS "sample.text_gt",
     (sample_arrays.int4_array >= $6) AS "sample.bool_gteq",
     ($7::integer = ANY(sample_arrays.int4_array)) AS "sample.int4_eq_any",
     ($8::integer != ANY(sample_arrays.int4_array)) AS "sample.int4_neq_any",
     ($9::integer = ALL(sample_arrays.int4_array)) AS "sample.int4_eq_all",
     (sample_arrays.int8_array @> $10) AS "sample.int8cont",
     (sample_arrays.int8_array <@ $11) AS "sample.int8cont_by",
     (sample_arrays.int4_array && $12) AS "sample.int4_overlap",
     (sample_arrays.bool_array || $13) AS "sample.bool_concat",
     (sample_arrays.text_array || $14::text) AS "sample.text_concat_el",
     sample_arrays.text_array[$15::integer] AS "sample.text_at",
     array_append(sample_arrays.text_array, $16::text) AS "sample.text_append",
     array_cat(sample_arrays.text_array, $17) AS "sample.text_cat",
     array_length(sample_arrays.text_array, $18::integer) AS "sample.text_length",
     array_prepend($19::text, sample_arrays.text_array) AS "sample.text_prepend"
FROM test_sample.sample_arrays
WHERE sample_arrays.bool_array @> $20;
`)

	type sample struct {
		model.SampleArrays
		TextEq       bool
		BoolEq       bool
		TextNeq      bool
		Int4Lt       bool
		Int8Lteq     bool
		TextGt       bool
		BoolGteq     bool
		Int4EqAny    bool
		Int4NeqAny   bool
		Int4EqAll    bool
		Int8Cont     bool
		Int8ContBy   bool
		Int4Overlap  bool
		BoolConcat   pq.BoolArray
		TextConcatEl pq.StringArray
		TextAt       string
		TextAppend   pq.StringArray
		TextCat      pq.StringArray
		TextLength   int32
		TextPrepend  pq.StringArray
	}

	var dest sample
	err := query.Query(db, &dest)
	require.NoError(t, err)

	expectedRow := sample{
		SampleArrays: sampleArrayRow,
		TextEq:       true,
		BoolEq:       true,
		TextNeq:      true,
		Int4Lt:       false,
		Int8Lteq:     true,
		TextGt:       true,
		BoolGteq:     true,
		Int4EqAny:    false,
		Int4NeqAny:   true,
		Int4EqAll:    false,
		Int8Cont:     false,
		Int8ContBy:   false,
		Int4Overlap:  true,
		BoolConcat:   pq.BoolArray{true, true},
		TextConcatEl: pq.StringArray{"a", "b", "z"},
		TextAt:       "a",
		TextAppend:   pq.StringArray{"a", "b", "after"},
		TextCat:      pq.StringArray{"a", "b", "a"},
		TextLength:   2,
		TextPrepend:  pq.StringArray{"before", "a", "b"},
	}

	testutils.AssertDeepEqual(t, dest, expectedRow, cmp.AllowUnexported(big.Int{}))
	requireLogged(t, query)
}

func TestArraySelectColumnsFromSubQuery(t *testing.T) {
	skipForCockroachDB(t)

	subQuery := SELECT(
		SampleArrays.AllColumns,
		SampleArrays.Int4Array.AS("array4"),
	).FROM(
		SampleArrays,
	).AsTable("sub_query")

	int4Array := IntegerArrayColumn("array4").From(subQuery)

	stmt := SELECT(
		subQuery.AllColumns(),
		int4Array,
	).FROM(
		subQuery,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT sub_query."sample_arrays.text_array" AS "sample_arrays.text_array",
     sub_query."sample_arrays.bool_array" AS "sample_arrays.bool_array",
     sub_query."sample_arrays.int4_array" AS "sample_arrays.int4_array",
     sub_query."sample_arrays.int8_array" AS "sample_arrays.int8_array",
     sub_query.array4 AS "array4",
     sub_query.array4 AS "array4"
FROM (
          SELECT sample_arrays.text_array AS "sample_arrays.text_array",
               sample_arrays.bool_array AS "sample_arrays.bool_array",
               sample_arrays.int4_array AS "sample_arrays.int4_array",
               sample_arrays.int8_array AS "sample_arrays.int8_array",
               sample_arrays.int4_array AS "array4"
          FROM test_sample.sample_arrays
     ) AS sub_query;
`)

	var dest struct {
		model.SampleArrays
		Array4 pq.Int32Array
	}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertDeepEqual(t, dest.SampleArrays.Int4Array, sampleArrayRow.Int4Array)
	testutils.AssertDeepEqual(t, dest.SampleArrays.Int8Array, sampleArrayRow.Int8Array)
	testutils.AssertDeepEqual(t, dest.Array4, sampleArrayRow.Int4Array)
}

func TestArrayTable_InsertColumn(t *testing.T) {
	skipForCockroachDB(t)

	insertQuery := SampleArrays.INSERT(SampleArrays.AllColumns).
		VALUES(
			ARRAY(String("A"), String("B")),
			ARRAY(Bool(true)),
			ARRAY(Int32(1)),
			ARRAY(Int64(2)),
		).
		MODEL(
			sampleArrayRow,
		).
		RETURNING(SampleArrays.AllColumns)

	expectedQuery := `
INSERT INTO test_sample.sample_arrays (text_array, bool_array, int4_array, int8_array)
VALUES (ARRAY['A'::text,'B'::text], ARRAY[TRUE::boolean], ARRAY[1::integer], ARRAY[2::bigint]),
       ('{"a","b"}', '{t}', '{1,2,3}', '{10,11,12}')
RETURNING sample_arrays.text_array AS "sample_arrays.text_array",
          sample_arrays.bool_array AS "sample_arrays.bool_array",
          sample_arrays.int4_array AS "sample_arrays.int4_array",
          sample_arrays.int8_array AS "sample_arrays.int8_array";
`
	testutils.AssertDebugStatementSql(t, insertQuery, expectedQuery)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var dest []model.SampleArrays
		err := insertQuery.Query(tx, &dest)
		require.NoError(t, err)
		require.Len(t, dest, 2)
		testutils.AssertDeepEqual(t, sampleArrayRow, dest[1], cmp.AllowUnexported(big.Int{}))
	})
}

func TestArrayTableUpdate(t *testing.T) {
	skipForCockroachDB(t)

	t.Run("using model", func(t *testing.T) {
		stmt := SampleArrays.UPDATE(SampleArrays.AllColumns).
			MODEL(sampleArrayRow).
			WHERE(String("a").EQ(ANY[StringExpression](SampleArrays.TextArray))).
			RETURNING(SampleArrays.AllColumns)

		testutils.AssertStatementSql(t, stmt, `
UPDATE test_sample.sample_arrays
SET (text_array, bool_array, int4_array, int8_array) = ($1, $2, $3, $4)
WHERE $5::text = ANY(sample_arrays.text_array)
RETURNING sample_arrays.text_array AS "sample_arrays.text_array",
          sample_arrays.bool_array AS "sample_arrays.bool_array",
          sample_arrays.int4_array AS "sample_arrays.int4_array",
          sample_arrays.int8_array AS "sample_arrays.int8_array";
`)

		testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
			var dest []model.SampleArrays
			err := stmt.Query(tx, &dest)
			require.NoError(t, err)
			require.Len(t, dest, 1)
			testutils.AssertDeepEqual(t, sampleArrayRow, dest[0], cmp.AllowUnexported(big.Int{}))
		})
	})

	t.Run("update using SET", func(t *testing.T) {
		stmt := SampleArrays.UPDATE().
			SET(
				SampleArrays.Int4Array.SET(ARRAY(Int32(-10), Int32(11))),
				SampleArrays.Int8Array.SET(ARRAY(Int64(-1200), Int64(7800))),
			).
			WHERE(String("a").EQ(ANY[StringExpression](SampleArrays.TextArray)))

		testutils.AssertDebugStatementSql(t, stmt, `
UPDATE test_sample.sample_arrays
SET int4_array = ARRAY[-10::integer,11::integer],
    int8_array = ARRAY[-1200::bigint,7800::bigint]
WHERE 'a'::text = ANY(sample_arrays.text_array);
`)

		testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
			testutils.AssertExec(t, stmt, tx, 1)
		})
	})

}

var sampleArrayRow = model.SampleArrays{
	TextArray: pq.StringArray([]string{"a", "b"}),
	BoolArray: pq.BoolArray([]bool{true}),
	Int4Array: pq.Int32Array([]int32{1, 2, 3}),
	Int8Array: pq.Int64Array([]int64{10, 11, 12}),
}
