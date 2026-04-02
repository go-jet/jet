package cubrid

import (
	"testing"
	"time"
)

func TestINTERVAL(t *testing.T) {
	assertSerialize(t, INTERVAL(15, SECOND), "INTERVAL 15 SECOND")
	assertSerialize(t, INTERVAL(1, MILLISECOND), "INTERVAL 1 MILLISECOND")
	assertSerialize(t, INTERVAL(2, MINUTE), "INTERVAL 2 MINUTE")
	assertSerialize(t, INTERVAL(3, HOUR), "INTERVAL 3 HOUR")
	assertSerialize(t, INTERVAL(4, DAY), "INTERVAL 4 DAY")
	assertSerialize(t, INTERVAL(1, WEEK), "INTERVAL 1 WEEK")
	assertSerialize(t, INTERVAL(5, MONTH), "INTERVAL 5 MONTH")
	assertSerialize(t, INTERVAL(6, YEAR), "INTERVAL 6 YEAR")
	assertSerialize(t, INTERVAL(-6, YEAR), "INTERVAL -6 YEAR")
	assertSerialize(t, INTERVAL(uint(6), YEAR), "INTERVAL 6 YEAR")
	assertSerialize(t, INTERVAL(int16(7), YEAR), "INTERVAL 7 YEAR")
	assertSerialize(t, INTERVAL(3.5, YEAR), "INTERVAL 3.5 YEAR")
}

func TestINTERVAL_InvalidType(t *testing.T) {
	assertPanicErr(t, func() { INTERVAL("11", HOUR) }, "jet: INTERVAL invalid value type. Numeric type expected")
}

func TestINTERVALe(t *testing.T) {
	assertSerialize(t, INTERVALe(table1ColFloat, SECOND), "INTERVAL table1.col_float SECOND")
	assertSerialize(t, INTERVALe(table1ColFloat, DAY), "INTERVAL table1.col_float DAY")
	assertSerialize(t, INTERVALe(table1ColFloat, YEAR), "INTERVAL table1.col_float YEAR")
}

func TestINTERVALd(t *testing.T) {
	assertSerialize(t, INTERVALd(3*time.Second), "INTERVAL 3 SECOND")
	assertSerialize(t, INTERVALd(-1*time.Second), "INTERVAL -1 SECOND")
	assertSerialize(t, INTERVALd(3*time.Minute), "INTERVAL 180 SECOND")
	assertSerialize(t, INTERVALd(2*time.Hour), "INTERVAL 7200 SECOND")
	assertSerialize(t, INTERVALd(24*time.Hour), "INTERVAL 86400 SECOND")
	assertSerialize(t, INTERVALd(3*time.Millisecond), "INTERVAL 3000 MILLISECOND")
	assertSerialize(t, INTERVALd(0), "INTERVAL 0 SECOND")
}

func TestUnitTypeString(t *testing.T) {
	if SECOND.String() != "SECOND" {
		t.Errorf("SECOND.String() = %q, want %q", SECOND.String(), "SECOND")
	}
	if DAY.String() != "DAY" {
		t.Errorf("DAY.String() = %q, want %q", DAY.String(), "DAY")
	}
}

func TestIntervalDebugString(t *testing.T) {
	s := intervalDebugString(5, SECOND)
	if s != "INTERVAL 5 SECOND" {
		t.Errorf("intervalDebugString(5, SECOND) = %q, want %q", s, "INTERVAL 5 SECOND")
	}
}
