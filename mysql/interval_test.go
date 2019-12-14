package mysql

import (
	"testing"
	"time"
)

func TestINTERVAL(t *testing.T) {
	assertSerialize(t, INTERVAL("3-2", YEAR_MONTH), "INTERVAL ? YEAR_MONTH")
	assertDebugSerialize(t, INTERVAL("3-2", YEAR_MONTH), "INTERVAL '3-2' YEAR_MONTH")
	assertDebugSerialize(t, INTERVAL("-3-2", YEAR_MONTH), "INTERVAL '-3-2' YEAR_MONTH")
	assertDebugSerialize(t, INTERVAL("10 25", DAY_HOUR), "INTERVAL '10 25' DAY_HOUR")
	assertDebugSerialize(t, INTERVAL("-10 25", DAY_HOUR), "INTERVAL '-10 25' DAY_HOUR")
	assertDebugSerialize(t, INTERVAL("10 25:15", DAY_MINUTE), "INTERVAL '10 25:15' DAY_MINUTE")
	assertDebugSerialize(t, INTERVAL("-10 25:15", DAY_MINUTE), "INTERVAL '-10 25:15' DAY_MINUTE")
	assertDebugSerialize(t, INTERVAL("10 25:15:08", DAY_SECOND), "INTERVAL '10 25:15:08' DAY_SECOND")
	assertDebugSerialize(t, INTERVAL("-10 25:15:08", DAY_SECOND), "INTERVAL '-10 25:15:08' DAY_SECOND")
	assertDebugSerialize(t, INTERVAL("10 25:15:08.000100", DAY_MICROSECOND), "INTERVAL '10 25:15:08.000100' DAY_MICROSECOND")
	assertDebugSerialize(t, INTERVAL("-10 25:15:08.000100", DAY_MICROSECOND), "INTERVAL '-10 25:15:08.000100' DAY_MICROSECOND")
	assertDebugSerialize(t, INTERVAL("15:08", HOUR_MINUTE), "INTERVAL '15:08' HOUR_MINUTE")
	assertDebugSerialize(t, INTERVAL("-15:08", HOUR_MINUTE), "INTERVAL '-15:08' HOUR_MINUTE")
	assertDebugSerialize(t, INTERVAL("15:08", HOUR_MINUTE), "INTERVAL '15:08' HOUR_MINUTE")
	assertDebugSerialize(t, INTERVAL("-15:08", HOUR_MINUTE), "INTERVAL '-15:08' HOUR_MINUTE")
	assertDebugSerialize(t, INTERVAL("15:08:03", HOUR_SECOND), "INTERVAL '15:08:03' HOUR_SECOND")
	assertDebugSerialize(t, INTERVAL("-15:08:03", HOUR_SECOND), "INTERVAL '-15:08:03' HOUR_SECOND")
	assertDebugSerialize(t, INTERVAL("25:15:08.000100", HOUR_MICROSECOND), "INTERVAL '25:15:08.000100' HOUR_MICROSECOND")
	assertDebugSerialize(t, INTERVAL("-25:15:08.000100", HOUR_MICROSECOND), "INTERVAL '-25:15:08.000100' HOUR_MICROSECOND")
	assertDebugSerialize(t, INTERVAL("08:03", MINUTE_SECOND), "INTERVAL '08:03' MINUTE_SECOND")
	assertDebugSerialize(t, INTERVAL("-08:03", MINUTE_SECOND), "INTERVAL '-08:03' MINUTE_SECOND")
	assertDebugSerialize(t, INTERVAL("15:08.000100", MINUTE_MICROSECOND), "INTERVAL '15:08.000100' MINUTE_MICROSECOND")
	assertDebugSerialize(t, INTERVAL("-15:08.000100", MINUTE_MICROSECOND), "INTERVAL '-15:08.000100' MINUTE_MICROSECOND")
	assertDebugSerialize(t, INTERVAL("08.000100", SECOND_MICROSECOND), "INTERVAL '08.000100' SECOND_MICROSECOND")
	assertDebugSerialize(t, INTERVAL("-08.000100", SECOND_MICROSECOND), "INTERVAL '-08.000100' SECOND_MICROSECOND")

	assertSerialize(t, INTERVAL(15, SECOND), "INTERVAL 15 SECOND")
	assertSerialize(t, INTERVAL(1, MICROSECOND), "INTERVAL 1 MICROSECOND")
	assertSerialize(t, INTERVAL(2, MINUTE), "INTERVAL 2 MINUTE")
	assertSerialize(t, INTERVAL(3, HOUR), "INTERVAL 3 HOUR")
	assertSerialize(t, INTERVAL(4, DAY), "INTERVAL 4 DAY")
	assertSerialize(t, INTERVAL(5, MONTH), "INTERVAL 5 MONTH")
	assertSerialize(t, INTERVAL(6, YEAR), "INTERVAL 6 YEAR")
	assertSerialize(t, INTERVAL(-6, YEAR), "INTERVAL -6 YEAR")

	assertSerialize(t, INTERVAL(uint(6), YEAR), "INTERVAL 6 YEAR")
	assertSerialize(t, INTERVAL(int16(7), YEAR), "INTERVAL 7 YEAR")
	assertSerialize(t, INTERVAL(3.5, YEAR), "INTERVAL 3.5 YEAR")
}

func TestINTERVAL_InvalidUnitType(t *testing.T) {
	assertPanicErr(t, func() { INTERVAL("11", HOUR) }, "jet: INTERVAL invalid value type. Numeric type expected")
	assertPanicErr(t, func() { INTERVAL("11", YEAR_MONTH) }, "jet: INTERVAL invalid format")
	assertPanicErr(t, func() { INTERVAL("11+11", YEAR_MONTH) }, "jet: INTERVAL invalid format")
	assertPanicErr(t, func() { INTERVAL(156.11, YEAR_MONTH) }, "jet: INTERNAL invalid value type. String type expected")
}

func TestINTERVALd(t *testing.T) {
	assertDebugSerialize(t, INTERVALd(3*time.Microsecond), "INTERVAL 3 MICROSECOND")
	assertDebugSerialize(t, INTERVALd(-1*time.Microsecond), "INTERVAL -1 MICROSECOND")

	assertDebugSerialize(t, INTERVALd(3*time.Second), "INTERVAL 3 SECOND")
	assertDebugSerialize(t, INTERVALd(3*time.Second+4*time.Microsecond), "INTERVAL '03.000004' SECOND_MICROSECOND")
	assertDebugSerialize(t, INTERVALd(-1*time.Second), "INTERVAL -1 SECOND")

	assertDebugSerialize(t, INTERVALd(3*time.Minute), "INTERVAL 3 MINUTE")
	assertDebugSerialize(t, INTERVALd(3*time.Minute+4*time.Second), "INTERVAL '03:04' MINUTE_SECOND")
	assertDebugSerialize(t, INTERVALd(3*time.Minute+4*time.Second+5*time.Microsecond), "INTERVAL '03:04.000005' MINUTE_MICROSECOND")
	assertDebugSerialize(t, INTERVALd(-11*time.Minute), "INTERVAL -11 MINUTE")
	assertDebugSerialize(t, INTERVALd(-11*time.Minute-22*time.Second), "INTERVAL '-11:22' MINUTE_SECOND")

	assertDebugSerialize(t, INTERVALd(3*time.Hour), "INTERVAL 3 HOUR")
	assertDebugSerialize(t, INTERVALd(3*time.Hour+4*time.Minute), "INTERVAL '03:04' HOUR_MINUTE")
	assertDebugSerialize(t, INTERVALd(3*time.Hour+4*time.Minute+5*time.Second), "INTERVAL '03:04:05' HOUR_SECOND")
	assertDebugSerialize(t, INTERVALd(3*time.Hour+4*time.Minute+5*time.Second+6*time.Millisecond), "INTERVAL '03:04:05.006000' HOUR_MICROSECOND")
	assertDebugSerialize(t, INTERVALd(-11*time.Hour), "INTERVAL -11 HOUR")
	assertDebugSerialize(t, INTERVALd(-11*time.Hour-22*time.Minute), "INTERVAL '-11:22' HOUR_MINUTE")

	assertDebugSerialize(t, INTERVALd(3*24*time.Hour), "INTERVAL 3 DAY")
	assertDebugSerialize(t, INTERVALd(3*24*time.Hour+4*time.Hour), "INTERVAL '3 04' DAY_HOUR")
	assertDebugSerialize(t, INTERVALd(3*24*time.Hour+4*time.Hour+5*time.Minute), "INTERVAL '3 04:05' DAY_MINUTE")
	assertDebugSerialize(t, INTERVALd(3*24*time.Hour+4*time.Hour+5*time.Minute+6*time.Second), "INTERVAL '3 04:05:06' DAY_SECOND")
	assertDebugSerialize(t, INTERVALd(3*24*time.Hour+4*time.Hour+5*time.Minute+6*time.Second+7*time.Microsecond), "INTERVAL '3 04:05:06.000007' DAY_MICROSECOND")

	assertDebugSerialize(t, INTERVALd(-11*24*time.Hour), "INTERVAL -11 DAY")

	assertDebugSerialize(t, INTERVALd(1*time.Hour+2*time.Minute+3*time.Second+345*time.Microsecond), "INTERVAL '01:02:03.000345' HOUR_MICROSECOND")
	assertDebugSerialize(t, INTERVALd(-1*(1*time.Hour+2*time.Minute+3*time.Second+345*time.Microsecond)), "INTERVAL '-1:02:03.000345' HOUR_MICROSECOND")
}

func TestINTERVALe(t *testing.T) {
	assertSerialize(t, INTERVALe(table1ColFloat, MICROSECOND), "INTERVAL table1.col_float MICROSECOND")
	assertSerialize(t, INTERVALe(table1ColFloat, SECOND), "INTERVAL table1.col_float SECOND")
	assertSerialize(t, INTERVALe(table1ColFloat, MINUTE), "INTERVAL table1.col_float MINUTE")
	assertSerialize(t, INTERVALe(table1ColFloat, HOUR), "INTERVAL table1.col_float HOUR")
	assertSerialize(t, INTERVALe(table1ColFloat, DAY), "INTERVAL table1.col_float DAY")
	assertSerialize(t, INTERVALe(table1ColFloat, WEEK), "INTERVAL table1.col_float WEEK")
	assertSerialize(t, INTERVALe(table1ColFloat, MONTH), "INTERVAL table1.col_float MONTH")
	assertSerialize(t, INTERVALe(table1ColFloat, QUARTER), "INTERVAL table1.col_float QUARTER")
	assertSerialize(t, INTERVALe(table1ColFloat, YEAR), "INTERVAL table1.col_float YEAR")
}
