//go:build postgres
// +build postgres

package postgres

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
	"time"

	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
)

func TestRangeTableSelect(t *testing.T) {
	skipForCockroachDB(t)

	int4Range := INT4_RANGE(Int(11), Int(20))
	int8Range := INT8_RANGE(Int(100), Int(250), String("[)"))
	numRange := NUM_RANGE(Int(11), Float(22.22))
	dateRange := DATE_RANGE(Date(2010, 10, 1), DateExp(PLUS_INFINITY))
	tsRange := TS_RANGE(
		Timestamp(2020, 02, 01, 0, 0, 0),
		Timestamp(2020, 10, 01, 0, 0, 0),
	)
	tstzRange := TSTZ_RANGE(
		TimestampzExp(MINUS_INFINITY),
		TimestampzT(time.Date(2014, 01, 01, 15, 0, 0, 0, time.UTC)),
	)

	query := SELECT(
		table.SampleRanges.AllColumns,

		table.SampleRanges.Int4Range.EQ(table.SampleRanges.Int4Range).AS("sample.int4eq"),
		table.SampleRanges.Int8Range.EQ(int8Range).AS("sample.int8eq"),
		table.SampleRanges.Int4Range.NOT_EQ(int4Range).AS("sample.int4neq"),
		table.SampleRanges.NumRange.LT(numRange).IS_TRUE().AS("sample.num_lt"),
		table.SampleRanges.DateRange.LT_EQ(dateRange).IS_FALSE().AS("sample.date_lteq"),
		table.SampleRanges.TimestampRange.GT(tsRange).AS("sample.ts_gt"),
		table.SampleRanges.TimestampzRange.GT_EQ(tstzRange).AS("sample.tstz_gteq"),
		table.SampleRanges.Int4Range.CONTAINS(Int32(22)).AS("sample.int4cont"),
		table.SampleRanges.Int8Range.CONTAINS(Int64(75364)).AS("sample.int8cont"),
		table.SampleRanges.Int8Range.CONTAINS_RANGE(int8Range).AS("sample.int8cont_range"),
		table.SampleRanges.NumRange.OVERLAP(numRange).AS("sample.num_overlap"),
		table.SampleRanges.DateRange.UNION(dateRange).AS("sample.date_union"),
		table.SampleRanges.TimestampRange.INTERSECTION(tsRange).AS("sample.ts_ints"),
		table.SampleRanges.TimestampzRange.DIFFERENCE(tstzRange).AS("sample.tstz_diff"),
		table.SampleRanges.Int4Range.UPPER_BOUND().ADD(Int(5)).AS("sample.int4_upper"),
		table.SampleRanges.Int8Range.LOWER_BOUND().SUB(Int(12)).AS("sample.int8_lower"),
		LOWER_BOUND[DateExpression](table.SampleRanges.DateRange),
		UPPER_BOUND[NumericExpression](table.SampleRanges.NumRange).AS("sample.num_upper"),
		table.SampleRanges.TimestampRange.UPPER_BOUND(),
		table.SampleRanges.DateRange.IS_EMPTY().AS("sample.date_empty"),
		table.SampleRanges.TimestampRange.LOWER_INC().AS("sample.ts_low_inc"),
		table.SampleRanges.TimestampzRange.UPPER_INC().AS("sample.tstz_up_inc"),
		table.SampleRanges.TimestampRange.LOWER_INF().AS("sample.ts_low_inf"),
		table.SampleRanges.TimestampzRange.UPPER_INF().AS("sample.tstz_up_inf"),

		RawInt4Range("'[1,2]'").EQ(int4Range),
		RawInt8Range("int8range(15, 25)").EQ(int8Range),
		RawNumRange("numrange(20.0, 30.0)").NOT_EQ(numRange),
	).FROM(
		table.SampleRanges,
	).WHERE(
		table.SampleRanges.DateRange.CONTAINS(Date(2023, 12, 12)),
	)

	testutils.AssertStatementSql(t, query, `
SELECT sample_ranges.date_range AS "sample_ranges.date_range",
     sample_ranges.timestamp_range AS "sample_ranges.timestamp_range",
     sample_ranges.timestampz_range AS "sample_ranges.timestampz_range",
     sample_ranges.int4_range AS "sample_ranges.int4_range",
     sample_ranges.int8_range AS "sample_ranges.int8_range",
     sample_ranges.num_range AS "sample_ranges.num_range",
     (sample_ranges.int4_range = sample_ranges.int4_range) AS "sample.int4eq",
     (sample_ranges.int8_range = int8range($1, $2, $3::text)) AS "sample.int8eq",
     (sample_ranges.int4_range != int4range($4, $5)) AS "sample.int4neq",
     (sample_ranges.num_range < numrange($6, $7)) IS TRUE AS "sample.num_lt",
     (sample_ranges.date_range <= daterange($8::date, $9)) IS FALSE AS "sample.date_lteq",
     (sample_ranges.timestamp_range > tsrange($10::timestamp without time zone, $11::timestamp without time zone)) AS "sample.ts_gt",
     (sample_ranges.timestampz_range >= tstzrange($12, $13::timestamp with time zone)) AS "sample.tstz_gteq",
     (sample_ranges.int4_range @> $14::integer) AS "sample.int4cont",
     (sample_ranges.int8_range @> $15::bigint) AS "sample.int8cont",
     (sample_ranges.int8_range @> int8range($16, $17, $18::text)) AS "sample.int8cont_range",
     (sample_ranges.num_range && numrange($19, $20)) AS "sample.num_overlap",
     (sample_ranges.date_range + daterange($21::date, $22)) AS "sample.date_union",
     (sample_ranges.timestamp_range * tsrange($23::timestamp without time zone, $24::timestamp without time zone)) AS "sample.ts_ints",
     (sample_ranges.timestampz_range - tstzrange($25, $26::timestamp with time zone)) AS "sample.tstz_diff",
     (UPPER(sample_ranges.int4_range) + $27) AS "sample.int4_upper",
     (LOWER(sample_ranges.int8_range) - $28) AS "sample.int8_lower",
     LOWER(sample_ranges.date_range),
     UPPER(sample_ranges.num_range) AS "sample.num_upper",
     UPPER(sample_ranges.timestamp_range),
     ISEMPTY(sample_ranges.date_range) AS "sample.date_empty",
     LOWER_INC(sample_ranges.timestamp_range) AS "sample.ts_low_inc",
     UPPER_INC(sample_ranges.timestampz_range) AS "sample.tstz_up_inc",
     LOWER_INF(sample_ranges.timestamp_range) AS "sample.ts_low_inf",
     UPPER_INF(sample_ranges.timestampz_range) AS "sample.tstz_up_inf",
     ('[1,2]') = int4range($29, $30),
     (int8range(15, 25)) = int8range($31, $32, $33::text),
     (numrange(20.0, 30.0)) != numrange($34, $35)
FROM test_sample.sample_ranges
WHERE sample_ranges.date_range @> $36::date;
`)

	type sample struct {
		model.SampleRanges

		Int4Eq        bool
		Int8Eq        bool
		Int4Neq       bool
		NumLt         bool
		DateLtEq      bool
		TsGt          bool
		TsTzGtEq      bool
		Int4Cont      bool
		Int8Cont      bool
		Int8ContRange bool
		NumOverlap    bool
		DateUnion     pgtype.Daterange
		TsInts        pgtype.Tsrange
		TsTzDiff      pgtype.Tstzrange
		Int4Upper     int32
		Int8Lower     int64
		NumUpper      float64
		DateEmpty     bool
		TsLowInc      bool
		TsTzUpInc     bool
		TsLowInf      bool
		TsTzUpInf     bool
	}

	var dest sample
	err := query.Query(db, &dest)
	require.NoError(t, err)

	expectedRow := sample{
		SampleRanges:  sampleRangeRow,
		Int4Eq:        true,
		Int8Eq:        false,
		Int4Neq:       false,
		NumLt:         false,
		DateLtEq:      true,
		TsGt:          false,
		TsTzGtEq:      true,
		Int4Cont:      false,
		Int8Cont:      false,
		Int8ContRange: false,
		NumOverlap:    false,
		DateUnion: pgtype.Daterange{
			Lower: pgtype.Date{
				Time:   time.Date(2010, 10, 1, 0, 0, 0, 0, time.UTC),
				Status: pgtype.Present,
			},
			Upper: pgtype.Date{
				Status:           pgtype.Present,
				InfinityModifier: pgtype.Infinity,
			},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Status:    pgtype.Present,
		},
		TsInts: pgtype.Tsrange{
			Lower: pgtype.Timestamp{
				Time:   time.Date(2020, 02, 01, 0, 0, 0, 0, time.UTC),
				Status: pgtype.Present,
			},
			Upper: pgtype.Timestamp{
				Time:   time.Date(2020, 10, 01, 0, 0, 0, 0, time.UTC),
				Status: pgtype.Present,
			},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Status:    pgtype.Present,
		},
		TsTzDiff: pgtype.Tstzrange{
			Lower: pgtype.Timestamptz{
				Time:   time.Date(2024, 05, 07, 15, 0, 0, 0, time.FixedZone("", 0)),
				Status: pgtype.Present,
			},
			Upper: pgtype.Timestamptz{
				Time:   time.Date(2024, 10, 11, 14, 0, 0, 0, time.FixedZone("", 0)),
				Status: pgtype.Present,
			},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Status:    pgtype.Present,
		},
		Int4Upper: 25,
		Int8Lower: 188,
		NumUpper:  5000,
		DateEmpty: false,
		TsLowInc:  true,
		TsTzUpInc: false,
		TsLowInf:  false,
		TsTzUpInf: false,
	}

	testutils.AssertDeepEqual(t, dest, expectedRow, cmp.AllowUnexported(big.Int{}))
	requireLogged(t, query)
}

func TestRangeSelectColumnsFromSubQuery(t *testing.T) {
	skipForCockroachDB(t)

	subQuery := SELECT(
		table.SampleRanges.AllColumns,
		table.SampleRanges.Int4Range.AS("range4"),
	).FROM(
		table.SampleRanges,
	).AsTable("sub_query")

	int4Range := Int4RangeColumn("range4").From(subQuery)

	stmt := SELECT(
		subQuery.AllColumns(),
		int4Range,
	).FROM(
		subQuery,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT sub_query."sample_ranges.date_range" AS "sample_ranges.date_range",
     sub_query."sample_ranges.timestamp_range" AS "sample_ranges.timestamp_range",
     sub_query."sample_ranges.timestampz_range" AS "sample_ranges.timestampz_range",
     sub_query."sample_ranges.int4_range" AS "sample_ranges.int4_range",
     sub_query."sample_ranges.int8_range" AS "sample_ranges.int8_range",
     sub_query."sample_ranges.num_range" AS "sample_ranges.num_range",
     sub_query.range4 AS "range4",
     sub_query.range4 AS "range4"
FROM (
          SELECT sample_ranges.date_range AS "sample_ranges.date_range",
               sample_ranges.timestamp_range AS "sample_ranges.timestamp_range",
               sample_ranges.timestampz_range AS "sample_ranges.timestampz_range",
               sample_ranges.int4_range AS "sample_ranges.int4_range",
               sample_ranges.int8_range AS "sample_ranges.int8_range",
               sample_ranges.num_range AS "sample_ranges.num_range",
               sample_ranges.int4_range AS "range4"
          FROM test_sample.sample_ranges
     ) AS sub_query;
`)

	var dest struct {
		model.SampleRanges

		Range4 pgtype.Int4range
	}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertDeepEqual(t, dest.SampleRanges.Int4Range, sampleRangeRow.Int4Range)
	testutils.AssertDeepEqual(t, dest.SampleRanges.Int8Range, sampleRangeRow.Int8Range)
	testutils.AssertDeepEqual(t, dest.Range4, sampleRangeRow.Int4Range)
}

func TestRangeTable_InsertColumn(t *testing.T) {
	skipForCockroachDB(t)

	insertQuery := table.SampleRanges.INSERT(table.SampleRanges.AllColumns).
		VALUES(
			DATE_RANGE(
				Date(2010, 01, 01),
				DateExp(PLUS_INFINITY),
				String("()"),
			),
			DEFAULT,
			TSTZ_RANGE(
				TimestampzExp(MINUS_INFINITY),
				TimestampzT(time.Date(2014, 01, 01, 15, 0, 0, 0, time.UTC)),
				String("[)"),
			),
			INT4_RANGE(Int(64), Int(128), String("[]")),
			INT8_RANGE(Int(1024), Int(2048), String("[]")),
			DEFAULT,
		).
		MODEL(
			sampleRangeRow,
		).RETURNING(table.SampleRanges.AllColumns)

	expectedQuery := `
INSERT INTO test_sample.sample_ranges (date_range, timestamp_range, timestampz_range, int4_range, int8_range, num_range)
VALUES (daterange('2010-01-01'::date, 'infinity', '()'::text), DEFAULT, tstzrange('-infinity', '2014-01-01 15:00:00Z'::timestamp with time zone, '[)'::text), int4range(64, 128, '[]'::text), int8range(1024, 2048, '[]'::text), DEFAULT),
       ('[2023-09-25,2024-02-10)', '[2020-01-01 00:00:00,2021-01-01 15:00:00]', '[2024-05-07 15:00:00Z,2024-10-11 14:00:00Z)', '[11,20)', '[200,2450)', '[2e3,5e3)')
RETURNING sample_ranges.date_range AS "sample_ranges.date_range",
          sample_ranges.timestamp_range AS "sample_ranges.timestamp_range",
          sample_ranges.timestampz_range AS "sample_ranges.timestampz_range",
          sample_ranges.int4_range AS "sample_ranges.int4_range",
          sample_ranges.int8_range AS "sample_ranges.int8_range",
          sample_ranges.num_range AS "sample_ranges.num_range";
`
	testutils.AssertDebugStatementSql(t, insertQuery, expectedQuery)

	testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
		var dest []model.SampleRanges
		err := insertQuery.Query(tx, &dest)
		require.NoError(t, err)
		require.Len(t, dest, 2)
		testutils.AssertDeepEqual(t, sampleRangeRow, dest[1], cmp.AllowUnexported(big.Int{}))
	})
}

func TestRangeTableUpdate(t *testing.T) {
	skipForCockroachDB(t)

	t.Run("using model", func(t *testing.T) {
		stmt := table.SampleRanges.UPDATE(table.SampleRanges.AllColumns).
			MODEL(sampleRangeRow).
			WHERE(table.SampleRanges.TimestampRange.LOWER_INF().IS_FALSE()).
			RETURNING(table.SampleRanges.AllColumns)

		testutils.AssertStatementSql(t, stmt, `
UPDATE test_sample.sample_ranges
SET (date_range, timestamp_range, timestampz_range, int4_range, int8_range, num_range) = ($1, $2, $3, $4, $5, $6)
WHERE LOWER_INF(sample_ranges.timestamp_range) IS FALSE
RETURNING sample_ranges.date_range AS "sample_ranges.date_range",
          sample_ranges.timestamp_range AS "sample_ranges.timestamp_range",
          sample_ranges.timestampz_range AS "sample_ranges.timestampz_range",
          sample_ranges.int4_range AS "sample_ranges.int4_range",
          sample_ranges.int8_range AS "sample_ranges.int8_range",
          sample_ranges.num_range AS "sample_ranges.num_range";
`)

		testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
			var dest []model.SampleRanges

			err := stmt.Query(tx, &dest)
			require.NoError(t, err)
			require.Len(t, dest, 1)
			testutils.AssertDeepEqual(t, sampleRangeRow, dest[0], cmp.AllowUnexported(big.Int{}))
		})
	})

	t.Run("update using SET", func(t *testing.T) {
		stmt := table.SampleRanges.UPDATE().
			SET(
				table.SampleRanges.Int4Range.SET(INT4_RANGE(Int32(-12), Int32(78))),
				table.SampleRanges.Int8Range.SET(INT8_RANGE(Int64(-1200), Int64(7800))),
			).
			WHERE(
				table.SampleRanges.TimestampzRange.LOWER_BOUND().GT(Timestampz(2024, 2, 27, 0, 0, 0, 0, "UTC")),
			)

		testutils.AssertDebugStatementSql(t, stmt, `
UPDATE test_sample.sample_ranges
SET int4_range = int4range(-12::integer, 78::integer),
    int8_range = int8range(-1200::bigint, 7800::bigint)
WHERE LOWER(sample_ranges.timestampz_range) > '2024-02-27 00:00:00 UTC'::timestamp with time zone;
`)

		testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
			testutils.AssertExec(t, stmt, tx, 1)
		})
	})

}

var sampleRangeRow = model.SampleRanges{
	DateRange: pgtype.Daterange{
		Lower: pgtype.Date{
			Time:   time.Date(2023, 9, 25, 0, 0, 0, 0, time.UTC),
			Status: pgtype.Present,
		},
		Upper: pgtype.Date{
			Time:   time.Date(2024, 2, 10, 0, 0, 0, 0, time.UTC),
			Status: pgtype.Present,
		},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Status:    pgtype.Present,
	},
	TimestampRange: pgtype.Tsrange{
		Lower: pgtype.Timestamp{
			Time:   time.Date(2020, 01, 01, 0, 0, 0, 0, time.UTC),
			Status: pgtype.Present,
		},
		Upper: pgtype.Timestamp{
			Time:   time.Date(2021, 01, 01, 15, 0, 0, 0, time.UTC),
			Status: pgtype.Present,
		},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Inclusive,
		Status:    pgtype.Present,
	},
	TimestampzRange: pgtype.Tstzrange{
		Lower: pgtype.Timestamptz{
			Time:   time.Date(2024, 05, 07, 15, 0, 0, 0, time.FixedZone("", 0)),
			Status: pgtype.Present,
		},
		Upper: pgtype.Timestamptz{
			Time:   time.Date(2024, 10, 11, 14, 0, 0, 0, time.FixedZone("", 0)),
			Status: pgtype.Present,
		},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Status:    pgtype.Present,
	},
	Int4Range: pgtype.Int4range{
		Lower: pgtype.Int4{
			Int:    11,
			Status: pgtype.Present,
		},
		Upper: pgtype.Int4{
			Int:    20,
			Status: pgtype.Present,
		},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Status:    pgtype.Present,
	},
	Int8Range: pgtype.Int8range{
		Lower: pgtype.Int8{
			Int:    200,
			Status: pgtype.Present,
		},
		Upper: pgtype.Int8{
			Int:    2450,
			Status: pgtype.Present,
		},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Status:    pgtype.Present,
	},
	NumRange: pgtype.Numrange{
		Lower: pgtype.Numeric{
			Int:    big.NewInt(2),
			Exp:    3,
			Status: pgtype.Present,
		},
		Upper: pgtype.Numeric{
			Int:    big.NewInt(5),
			Status: pgtype.Present,
			Exp:    3,
		},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Status:    pgtype.Present,
	},
}
