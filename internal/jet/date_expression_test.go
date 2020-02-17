package jet

import (
	"testing"
)

func TestDateArithmetic(t *testing.T) {
	timestamp := Timestamp(2000, 1, 1, 0, 0, 0)
	assertClauseDebugSerialize(t, table1ColDate.ADD(NewInterval(String("1 HOUR"))).EQ(timestamp),
		"((table1.col_date + INTERVAL '1 HOUR') = '2000-01-01 00:00:00')")
	assertClauseDebugSerialize(t, table1ColDate.SUB(NewInterval(String("1 HOUR"))).EQ(timestamp),
		"((table1.col_date - INTERVAL '1 HOUR') = '2000-01-01 00:00:00')")
}
