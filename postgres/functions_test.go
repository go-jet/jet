package postgres

import (
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
