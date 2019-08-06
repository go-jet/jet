package postgres

import "testing"

func TestDateLiteral(t *testing.T) {
	assertClauseSerialize(t, Date(2019, 8, 6), "$1::DATE", "2019-08-06")
}
