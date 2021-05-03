package postgres

import "testing"

func TestLATERAL(t *testing.T) {
	assertSerialize(t,
		LATERAL(
			SELECT(Int(1)),
		).AS("lat1"),

		`LATERAL (
     SELECT $1
) AS lat1`)
}
