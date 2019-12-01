package postgres

import (
	"testing"
	"time"
)

func TestINTERVAL(t *testing.T) {
	assertSerialize(t, INTERVAL(1, YEAR), "INTERVAL '1 YEAR'")
	assertSerialize(t, INTERVAL(1, MONTH), "INTERVAL '1 MONTH'")
	assertSerialize(t, INTERVAL(1, WEEK), "INTERVAL '1 WEEK'")
	assertSerialize(t, INTERVAL(1, DAY), "INTERVAL '1 DAY'")
	assertSerialize(t, INTERVAL(1, HOUR), "INTERVAL '1 HOUR'")
	assertSerialize(t, INTERVAL(1, MINUTE), "INTERVAL '1 MINUTE'")
	assertSerialize(t, INTERVAL(1, SECOND), "INTERVAL '1 SECOND'")
	assertSerialize(t, INTERVAL(1, MILLISECOND), "INTERVAL '1 MILLISECOND'")
	assertSerialize(t, INTERVAL(1, MICROSECOND), "INTERVAL '1 MICROSECOND'")
	assertSerialize(t, INTERVAL(1, DECADE), "INTERVAL '1 DECADE'")
	assertSerialize(t, INTERVAL(1, CENTURY), "INTERVAL '1 CENTURY'")
	assertSerialize(t, INTERVAL(1, MILLENNIUM), "INTERVAL '1 MILLENNIUM'")

	assertSerialize(t, INTERVAL(1, YEAR, 10, MONTH), "INTERVAL '1 YEAR 10 MONTH'")
	assertSerialize(t, INTERVAL(1, YEAR, 10, MONTH, 20, DAY), "INTERVAL '1 YEAR 10 MONTH 20 DAY'")
	assertSerialize(t, INTERVAL(1, YEAR, 10, MONTH, 20, DAY, 3, HOUR), "INTERVAL '1 YEAR 10 MONTH 20 DAY 3 HOUR'")

	assertSerialize(t, INTERVAL(1, YEAR).IS_NOT_NULL(), "INTERVAL '1 YEAR' IS NOT NULL")
	assertProjectionSerialize(t, INTERVAL(1, YEAR).AS("one year"), `INTERVAL '1 YEAR' AS "one year"`)
}

func TestINTERVALd(t *testing.T) {
	assertSerialize(t, INTERVALd(0), "INTERVAL '0 MICROSECOND'")
	assertSerialize(t, INTERVALd(1*time.Microsecond), "INTERVAL '1 MICROSECOND'")
	assertSerialize(t, INTERVALd(1*time.Millisecond), "INTERVAL '1000 MICROSECOND'")
	assertSerialize(t, INTERVALd(1*time.Second), "INTERVAL '1 SECOND'")
	assertSerialize(t, INTERVALd(1*time.Minute), "INTERVAL '1 MINUTE'")
	assertSerialize(t, INTERVALd(1*time.Hour), "INTERVAL '1 HOUR'")
	assertSerialize(t, INTERVALd(24*time.Hour), "INTERVAL '1 DAY'")

	assertSerialize(t, INTERVALd(24*time.Hour+2*time.Hour+3*time.Minute+4*time.Second+5*time.Microsecond),
		"INTERVAL '1 DAY 2 HOUR 3 MINUTE 4 SECOND 5 MICROSECOND'")
}

func TestINTERVAL_InvalidParams(t *testing.T) {
	assertPanicErr(t, func() { INTERVAL(1) }, "jet: invalid number of quantity and unit fields")
	assertPanicErr(t, func() { INTERVAL(1, 2) }, "jet: invalid INTERVAL unit type")
}

func TestIntervalArithmetic(t *testing.T) {
	assertSerialize(t, table2ColDate.ADD(INTERVAL(1, HOUR)), "(table2.col_date + INTERVAL '1 HOUR')")
	assertSerialize(t, table2ColDate.SUB(INTERVAL(1, HOUR)), "(table2.col_date - INTERVAL '1 HOUR')")
	assertSerialize(t, table2ColTime.ADD(INTERVAL(1, HOUR)), "(table2.col_time + INTERVAL '1 HOUR')")
	assertSerialize(t, table2ColTime.SUB(INTERVAL(1, HOUR)), "(table2.col_time - INTERVAL '1 HOUR')")
	assertSerialize(t, table2ColTimez.ADD(INTERVAL(1, HOUR)), "(table2.col_timez + INTERVAL '1 HOUR')")
	assertSerialize(t, table2ColTimez.SUB(INTERVAL(1, HOUR)), "(table2.col_timez - INTERVAL '1 HOUR')")
	assertSerialize(t, table2ColTimestamp.ADD(INTERVAL(1, HOUR)), "(table2.col_timestamp + INTERVAL '1 HOUR')")
	assertSerialize(t, table2ColTimestamp.SUB(INTERVAL(1, HOUR)), "(table2.col_timestamp - INTERVAL '1 HOUR')")
	assertSerialize(t, table2ColTimestampz.ADD(INTERVAL(1, HOUR)), "(table2.col_timestampz + INTERVAL '1 HOUR')")
	assertSerialize(t, table2ColTimestampz.SUB(INTERVAL(1, HOUR)), "(table2.col_timestampz - INTERVAL '1 HOUR')")
}
