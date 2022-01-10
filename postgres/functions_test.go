package postgres

import "testing"

func TestROW(t *testing.T) {
	assertSerialize(t, ROW(SELECT(Int(1))), `ROW((
     SELECT $1
))`)
	assertSerialize(t, ROW(Int(1), SELECT(Int(2)), Float(11.11)), `ROW($1, (
     SELECT $2
), $3)`)
}
