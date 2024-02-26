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
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
)

func TestRangeTable_DateContainsSingle(t *testing.T) {
	skipForCockroachDB(t)
	expectedSQL := `
SELECT DISTINCT sample_ranges.date_range AS "sample_ranges.date_range",
     sample_ranges.timestamp_range AS "sample_ranges.timestamp_range",
     sample_ranges.timestampz_range AS "sample_ranges.timestampz_range",
     sample_ranges.int4_range AS "sample_ranges.int4_range",
     sample_ranges.int8_range AS "sample_ranges.int8_range",
     sample_ranges.num_range AS "sample_ranges.num_range"
FROM test_sample.sample_ranges
WHERE sample_ranges.date_range @> '2023-12-12'::date;
`

	query := SELECT(SampleRanges.AllColumns).
		DISTINCT().
		FROM(SampleRanges).
		WHERE(SampleRanges.DateRange.CONTAINS(Date(2023, 12, 12)))

	testutils.AssertDebugStatementSql(t, query, expectedSQL, "2023-12-12")

	sample := model.SampleRanges{}
	err := query.Query(db, &sample)

	require.NoError(t, err)

	expectedRow := model.SampleRanges{
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

	testutils.AssertDeepEqual(t, sample, expectedRow, cmp.AllowUnexported(big.Int{}))
	requireLogged(t, query)
}

func TestRangeTable_IntContainsRange(t *testing.T) {
	skipForCockroachDB(t)
	expectedSQL := `
SELECT DISTINCT sample_ranges.date_range AS "sample_ranges.date_range",
     sample_ranges.timestamp_range AS "sample_ranges.timestamp_range",
     sample_ranges.timestampz_range AS "sample_ranges.timestampz_range",
     sample_ranges.int4_range AS "sample_ranges.int4_range",
     sample_ranges.int8_range AS "sample_ranges.int8_range",
     sample_ranges.num_range AS "sample_ranges.num_range"
FROM test_sample.sample_ranges
WHERE sample_ranges.int4_range @> int4range(12, 18, '[)'::text);
`

	query := SELECT(SampleRanges.AllColumns).
		DISTINCT().
		FROM(SampleRanges).
		WHERE(SampleRanges.Int4Range.CONTAINS_RANGE(INT4_RANGE(Int(12), Int(18), String("[)"))))

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(12), int64(18), "[)")

	sample := model.SampleRanges{}
	err := query.Query(db, &sample)

	require.NoError(t, err)

	expectedRow := model.SampleRanges{
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

	testutils.AssertDeepEqual(t, sample, expectedRow, cmp.AllowUnexported(big.Int{}))
	requireLogged(t, query)
}

func TestRangeTable_TimestampContainsRange(t *testing.T) {
	skipForCockroachDB(t)
	expectedSQL := `
SELECT DISTINCT sample_ranges.date_range AS "sample_ranges.date_range",
     sample_ranges.timestamp_range AS "sample_ranges.timestamp_range",
     sample_ranges.timestampz_range AS "sample_ranges.timestampz_range",
     sample_ranges.int4_range AS "sample_ranges.int4_range",
     sample_ranges.int8_range AS "sample_ranges.int8_range",
     sample_ranges.num_range AS "sample_ranges.num_range"
FROM test_sample.sample_ranges
WHERE sample_ranges.timestamp_range @> tsrange('2020-02-01 00:00:00'::timestamp without time zone, '2020-10-01 00:00:00'::timestamp without time zone, '[)'::text);
`

	query := SELECT(SampleRanges.AllColumns).
		DISTINCT().
		FROM(SampleRanges).
		WHERE(SampleRanges.TimestampRange.CONTAINS_RANGE(TIMESTAMP_RANGE(Timestamp(2020, 02, 01, 0, 0, 0), Timestamp(2020, 10, 01, 0, 0, 0), String("[)"))))

	testutils.AssertDebugStatementSql(t, query, expectedSQL, "2020-02-01 00:00:00", "2020-10-01 00:00:00", "[)")

	sample := model.SampleRanges{}
	err := query.Query(db, &sample)

	require.NoError(t, err)

	expectedRow := model.SampleRanges{
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

	testutils.AssertDeepEqual(t, sample, expectedRow, cmp.AllowUnexported(big.Int{}))
	requireLogged(t, query)
}

func TestRangeTable_ContainsOutOfRange(t *testing.T) {
	skipForCockroachDB(t)
	expectedSQL := `
SELECT DISTINCT sample_ranges.date_range AS "sample_ranges.date_range",
     sample_ranges.timestamp_range AS "sample_ranges.timestamp_range",
     sample_ranges.timestampz_range AS "sample_ranges.timestampz_range",
     sample_ranges.int4_range AS "sample_ranges.int4_range",
     sample_ranges.int8_range AS "sample_ranges.int8_range",
     sample_ranges.num_range AS "sample_ranges.num_range"
FROM test_sample.sample_ranges
WHERE sample_ranges.int4_range @> int4range(12, 30, '[)'::text);
`

	query := SELECT(SampleRanges.AllColumns).
		DISTINCT().
		FROM(SampleRanges).
		WHERE(SampleRanges.Int4Range.CONTAINS_RANGE(INT4_RANGE(Int(12), Int(30), String("[)"))))

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(12), int64(30), "[)")

	sample := model.SampleRanges{}
	err := query.Query(db, &sample)

	require.ErrorIs(t, err, qrm.ErrNoRows)
	requireLogged(t, query)
}

func TestRangeTable_InsertColumn(t *testing.T) {
	skipForCockroachDB(t)

	insertQuery := SampleRanges.INSERT(SampleRanges.AllColumns).
		VALUES(
			DATE_RANGE(
				Date(2010, 01, 01),
				Date(2014, 01, 01),
				String("[)"),
			),
			DEFAULT,
			TIMESTAMPTZ_RANGE(
				TimestampzT(time.Date(2010, 01, 01, 23, 0, 0, 0, time.UTC)),
				TimestampzT(time.Date(2014, 01, 01, 15, 0, 0, 0, time.UTC)),
				String("[)"),
			),
			INT4_RANGE(Int(64), Int(128), String("[]")),
			INT8_RANGE(Int(1024), Int(2048), String("[]")),
			DEFAULT,
		).
		RETURNING(SampleRanges.AllColumns)

	expectedQuery := `
INSERT INTO test_sample.sample_ranges (date_range, timestamp_range, timestampz_range, int4_range, int8_range, num_range)
VALUES (daterange('2010-01-01'::date, '2014-01-01'::date, '[)'::text), DEFAULT, tstzrange('2010-01-01 23:00:00Z'::timestamp with time zone, '2014-01-01 15:00:00Z'::timestamp with time zone, '[)'::text), int4range(64, 128, '[]'::text), int8range(1024, 2048, '[]'::text), DEFAULT)
RETURNING sample_ranges.date_range AS "sample_ranges.date_range",
          sample_ranges.timestamp_range AS "sample_ranges.timestamp_range",
          sample_ranges.timestampz_range AS "sample_ranges.timestampz_range",
          sample_ranges.int4_range AS "sample_ranges.int4_range",
          sample_ranges.int8_range AS "sample_ranges.int8_range",
          sample_ranges.num_range AS "sample_ranges.num_range";
`
	testutils.AssertDebugStatementSql(t, insertQuery, expectedQuery,
		"2010-01-01", "2014-01-01", "[)",
		time.Date(2010, 01, 01, 23, 0, 0, 0, time.UTC), time.Date(2014, 01, 01, 15, 0, 0, 0, time.UTC), "[)",
		int64(64), int64(128), "[]",
		int64(1024), int64(2048), "[]",
	)
}

func TestRangeTable_UpperBound(t *testing.T) {
	skipForCockroachDB(t)

	expectedSQL := `
SELECT UPPER(sample_ranges.date_range)
FROM test_sample.sample_ranges
WHERE sample_ranges.date_range @> '2023-12-12'::date;
`

	query := SELECT(UPPER_BOUND[DateExpression](SampleRanges.DateRange)).
		FROM(SampleRanges).
		WHERE(SampleRanges.DateRange.CONTAINS(Date(2023, 12, 12)))

	testutils.AssertDebugStatementSql(t, query, expectedSQL, "2023-12-12")

	var date time.Time
	err := query.Query(db, &date)
	require.NoError(t, err)

	expectedYear := 2024
	expectedMonth := time.February
	expectedDay := 10
	if expectedYear != date.Year() || expectedMonth != date.Month() || expectedDay != date.Day() {
		t.Errorf("expected: 2024-02-10 got: %s", date.Format("2006-01-02"))
	}
}

func TestRangeTable_LowerBound(t *testing.T) {
	skipForCockroachDB(t)

	expectedSQL := `
SELECT LOWER(sample_ranges.date_range)
FROM test_sample.sample_ranges
WHERE sample_ranges.date_range @> '2023-12-12'::date;
`

	query := SELECT(LOWER_BOUND[DateExpression](SampleRanges.DateRange)).
		FROM(SampleRanges).
		WHERE(SampleRanges.DateRange.CONTAINS(Date(2023, 12, 12)))

	testutils.AssertDebugStatementSql(t, query, expectedSQL, "2023-12-12")

	var date time.Time
	err := query.Query(db, &date)
	require.NoError(t, err)

	expectedYear := 2023
	expectedMonth := time.September
	expectedDay := 25
	if expectedYear != date.Year() || expectedMonth != date.Month() || expectedDay != date.Day() {
		t.Errorf("expected: 2023-09-25 got: %s", date.Format("2006-01-02"))
	}
}

func TestRangeTable_InsertInfinite(t *testing.T) {
	skipForCockroachDB(t)

	insertQuery := SampleRanges.INSERT(SampleRanges.AllColumns).
		VALUES(
			DATE_RANGE(
				Date(2010, 01, 01),
				DateExp(PLUS_INFINITY),
				String("[)"),
			),
			DEFAULT,
			TIMESTAMPTZ_RANGE(
				TimestampzExp(MINUS_INFINITY),
				TimestampzT(time.Date(2014, 01, 01, 15, 0, 0, 0, time.UTC)),
				String("[)"),
			),
			INT4_RANGE(Int(64), Int(128), String("[]")),
			INT8_RANGE(Int(1024), Int(2048), String("[]")),
			DEFAULT,
		).
		RETURNING(SampleRanges.AllColumns)

	expectedQuery := `
INSERT INTO test_sample.sample_ranges (date_range, timestamp_range, timestampz_range, int4_range, int8_range, num_range)
VALUES (daterange('2010-01-01'::date, 'infinity', '[)'::text), DEFAULT, tstzrange('-infinity', '2014-01-01 15:00:00Z'::timestamp with time zone, '[)'::text), int4range(64, 128, '[]'::text), int8range(1024, 2048, '[]'::text), DEFAULT)
RETURNING sample_ranges.date_range AS "sample_ranges.date_range",
          sample_ranges.timestamp_range AS "sample_ranges.timestamp_range",
          sample_ranges.timestampz_range AS "sample_ranges.timestampz_range",
          sample_ranges.int4_range AS "sample_ranges.int4_range",
          sample_ranges.int8_range AS "sample_ranges.int8_range",
          sample_ranges.num_range AS "sample_ranges.num_range";
`

	testutils.AssertDebugStatementSql(t, insertQuery, expectedQuery,
		"2010-01-01", "infinity", "[)",
		"-infinity", time.Date(2014, 01, 01, 15, 0, 0, 0, time.UTC), "[)",
		int64(64), int64(128), "[]",
		int64(1024), int64(2048), "[]",
	)
}
