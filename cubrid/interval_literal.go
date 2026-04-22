package cubrid

import (
	"fmt"
	"time"

	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/internal/utils/datetime"
)

// Interval is representation of CUBRID interval
type Interval = jet.Interval

// INTERVAL creates new temporal interval.
// CUBRID supports single-unit intervals:
//
//	INTERVAL(1, DAY)
//	INTERVAL(30, MINUTE)
func INTERVAL(value interface{}, unit unitType) Interval {
	if !isNumericType(value) {
		panic("jet: INTERVAL invalid value type. Numeric type expected")
	}
	return INTERVALe(jet.FixedLiteral(value), unit)
}

// INTERVALe creates new temporal interval from expression and unit type.
func INTERVALe(expr Expression, unit unitType) Interval {
	return jet.IntervalExp(CustomExpression(Token("INTERVAL"), expr, Token(string(unit))))
}

// INTERVALd creates new temporal interval from time.Duration.
func INTERVALd(duration time.Duration) Interval {
	var sign int64 = 1
	if duration < 0 {
		sign = -1
		duration = -duration
	}

	days, hours, minutes, sec, microsec := datetime.ExtractTimeComponents(duration)

	if days != 0 {
		return INTERVAL(sign*(days*24*60*60+hours*60*60+minutes*60+sec), SECOND)
	}
	if hours != 0 {
		totalSec := hours*3600 + minutes*60 + sec
		if microsec > 0 {
			return INTERVAL(sign*totalSec, SECOND)
		}
		return INTERVAL(sign*totalSec, SECOND)
	}
	if minutes != 0 {
		totalSec := minutes*60 + sec
		return INTERVAL(sign*totalSec, SECOND)
	}
	if sec != 0 {
		return INTERVAL(sign*sec, SECOND)
	}
	if microsec != 0 {
		return INTERVAL(sign*microsec, MILLISECOND)
	}

	return INTERVAL(int64(0), SECOND)
}

func isNumericType(value interface{}) bool {
	switch value.(type) {
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

// unitType constants are already defined in functions.go:
// YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND

// Additional interval units for CUBRID
const (
	WEEK unitType = "WEEK"
)

// String representation for debugging
func (u unitType) String() string {
	return string(u)
}

// Helper for debug output
func intervalDebugString(value interface{}, unit unitType) string {
	return fmt.Sprintf("INTERVAL %v %s", value, unit)
}
