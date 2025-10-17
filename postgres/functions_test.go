package postgres

import (
	"github.com/lib/pq"
	"testing"
)

func TestROW(t *testing.T) {
	assertSerialize(t, ROW(SELECT(Int(1))), `ROW((
     SELECT $1
))`)
	assertSerialize(t, ROW(Int(1), SELECT(Int(2)), Float(11.11)), `ROW($1, (
     SELECT $2
), $3)`)
}

func TestDATE_TRUNC(t *testing.T) {
	assertSerialize(t, DATE_TRUNC(YEAR, NOW()), "DATE_TRUNC('YEAR', NOW())")
	assertSerialize(
		t,
		DATE_TRUNC(DAY, NOW().ADD(INTERVAL(1, HOUR)), "Australia/Sydney"),
		"DATE_TRUNC('DAY', NOW() + INTERVAL '1 HOUR', 'Australia/Sydney')",
	)
}

func TestGENERATE_SERIES(t *testing.T) {
	assertSerialize(
		t,
		GENERATE_SERIES(NOW(), NOW().ADD(INTERVAL(10, DAY))),
		"GENERATE_SERIES(NOW(), NOW() + INTERVAL '10 DAY')",
	)
	assertSerialize(
		t,
		GENERATE_SERIES(NOW(), NOW().ADD(INTERVAL(10, DAY)), INTERVAL(2, DAY)),
		"GENERATE_SERIES(NOW(), NOW() + INTERVAL '10 DAY', INTERVAL '2 DAY')",
	)
}

func TestArrayFunctions(t *testing.T) {

	intArray := Int32Array(1, 2, 3)
	stringArrayColumn := StringArrayColumn("str_arr_col")

	assertSerialize(t, ARRAY_LOWER(intArray), "ARRAY_LOWER($1::integer[])", pq.Int32Array{1, 2, 3})
	assertSerialize(t, ARRAY_DIMS(stringArrayColumn).EQ(String("[1,1]")),
		"(ARRAY_DIMS(str_arr_col) = $1::text)",
		"[1,1]",
	)
	assertSerialize(t, ARRAY_NDIMS(stringArrayColumn).EQ(Int(1)), "(ARRAY_NDIMS(str_arr_col) = $1)", int64(1))
	assertSerialize(t, ARRAY_REVERSE(stringArrayColumn), "ARRAY_REVERSE(str_arr_col)")
	assertSerialize(t, ARRAY_SAMPLE(stringArrayColumn, Int(2)).AT(Int(1)).EQ(String("john")),
		"(ARRAY_SAMPLE(str_arr_col, $1)[$2] = $3::text)",
		int64(2), int64(1), "john",
	)

	assertSerialize(t, ARRAY_SHUFFLE(intArray).AT(Int(2)).EQ(Int(33)),
		"(ARRAY_SHUFFLE($1::integer[])[$2] = $3)",
		pq.Int32Array{1, 2, 3}, int64(2), int64(33),
	)

	assertSerialize(t, ARRAY_SORT(stringArrayColumn, Bool(true)),
		"ARRAY_SORT(str_arr_col, $1::boolean)",
		true,
	)

	assertSerialize(t, ARRAY_SORT(stringArrayColumn, Bool(true), Bool(false)),
		"ARRAY_SORT(str_arr_col, $1::boolean, $2::boolean)",
		true, false,
	)

	assertSerialize(t, TRIM_ARRAY(stringArrayColumn, Int(6)),
		"TRIM_ARRAY(str_arr_col, $1)",
		int64(6),
	)
}
