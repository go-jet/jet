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

	f := 5.2
	assertSerialize(t, INTERVAL(f, YEAR), "INTERVAL '5.2 YEAR'")
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
	assertPanicErr(t, func() { INTERVAL() }, "jet: invalid number of quantity and unit fields")
	assertPanicErr(t, func() { INTERVAL(1) }, "jet: invalid number of quantity and unit fields")
	assertPanicErr(t, func() { INTERVAL(1, 2) }, "jet: invalid INTERVAL unit type")
}

func TestDateTimeIntervalArithmetic(t *testing.T) {
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

func TestIntervalExpressionMethods(t *testing.T) {
	assertSerialize(t, table1ColInterval.EQ(table2ColInterval), "(table1.col_interval = table2.col_interval)")
	assertSerialize(t, table1ColInterval.EQ(INTERVAL(10, SECOND)), "(table1.col_interval = INTERVAL '10 SECOND')")
	assertSerialize(t, table1ColInterval.EQ(INTERVALd(11*time.Minute)), "(table1.col_interval = INTERVAL '11 MINUTE')")
	assertSerialize(t, table1ColInterval.EQ(INTERVALd(11*time.Minute)).EQ(Bool(false)),
		"((table1.col_interval = INTERVAL '11 MINUTE') = $1::boolean)", false)
	assertSerialize(t, table1ColInterval.NOT_EQ(table2ColInterval), "(table1.col_interval != table2.col_interval)")
	assertSerialize(t, table1ColInterval.IS_DISTINCT_FROM(table2ColInterval), "(table1.col_interval IS DISTINCT FROM table2.col_interval)")
	assertSerialize(t, table1ColInterval.IS_NOT_DISTINCT_FROM(table2ColInterval), "(table1.col_interval IS NOT DISTINCT FROM table2.col_interval)")
	assertSerialize(t, table1ColInterval.LT(table2ColInterval), "(table1.col_interval < table2.col_interval)")
	assertSerialize(t, table1ColInterval.LT_EQ(table2ColInterval), "(table1.col_interval <= table2.col_interval)")
	assertSerialize(t, table1ColInterval.GT(table2ColInterval), "(table1.col_interval > table2.col_interval)")
	assertSerialize(t, table1ColInterval.GT_EQ(table2ColInterval), "(table1.col_interval >= table2.col_interval)")
	assertSerialize(t, table1ColInterval.ADD(table2ColInterval), "(table1.col_interval + table2.col_interval)")
	assertSerialize(t, table1ColInterval.SUB(table2ColInterval), "(table1.col_interval - table2.col_interval)")
	assertSerialize(t, table1ColInterval.MUL(table2ColInt), "(table1.col_interval * table2.col_int)")
	assertSerialize(t, table1ColInterval.MUL(table2ColFloat), "(table1.col_interval * table2.col_float)")
	assertSerialize(t, table1ColInterval.DIV(table2ColInt), "(table1.col_interval / table2.col_int)")
	assertSerialize(t, table1ColInterval.DIV(table2ColFloat), "(table1.col_interval / table2.col_float)")
}
