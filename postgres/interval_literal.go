package postgres

import (
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils/datetime"
	"strconv"
	"strings"
	"time"
)

type quantityAndUnit = float64
type unit = float64

// Interval unit types
const (
	YEAR unit = 123456789 + iota
	MONTH
	WEEK
	DAY
	HOUR
	MINUTE
	SECOND
	MILLISECOND
	MICROSECOND
	DECADE
	CENTURY
	MILLENNIUM
)

// INTERVAL creates new interval expression from the list of quantity-unit pairs.
//
//	INTERVAL(1, DAY, 3, MINUTE)
func INTERVAL(quantityAndUnit ...quantityAndUnit) IntervalExpression {
	quantityAndUnitLen := len(quantityAndUnit)
	if quantityAndUnitLen == 0 || quantityAndUnitLen%2 != 0 {
		panic("jet: invalid number of quantity and unit fields")
	}

	var fields []string

	for i := 0; i < len(quantityAndUnit); i += 2 {
		quantity := strconv.FormatFloat(quantityAndUnit[i], 'f', -1, 64)
		unitString := unitToString(quantityAndUnit[i+1])
		fields = append(fields, quantity+" "+unitString)
	}

	return IntervalExp(CustomExpression(Token(fmt.Sprintf("INTERVAL '%s'", strings.Join(fields, " ")))))
}

// INTERVALd creates interval expression from time.Duration
func INTERVALd(duration time.Duration) IntervalExpression {
	days, hours, minutes, seconds, microseconds := datetime.ExtractTimeComponents(duration)

	var quantityAndUnits []quantityAndUnit

	if days > 0 {
		quantityAndUnits = append(quantityAndUnits, quantityAndUnit(days))
		quantityAndUnits = append(quantityAndUnits, DAY)
	}

	if hours > 0 {
		quantityAndUnits = append(quantityAndUnits, quantityAndUnit(hours))
		quantityAndUnits = append(quantityAndUnits, HOUR)
	}

	if minutes > 0 {
		quantityAndUnits = append(quantityAndUnits, quantityAndUnit(minutes))
		quantityAndUnits = append(quantityAndUnits, MINUTE)
	}

	if seconds > 0 {
		quantityAndUnits = append(quantityAndUnits, quantityAndUnit(seconds))
		quantityAndUnits = append(quantityAndUnits, SECOND)
	}

	if microseconds > 0 {
		quantityAndUnits = append(quantityAndUnits, quantityAndUnit(microseconds))
		quantityAndUnits = append(quantityAndUnits, MICROSECOND)
	}

	if len(quantityAndUnits) == 0 {
		return INTERVAL(0, MICROSECOND)
	}

	return INTERVAL(quantityAndUnits...)
}

func unitToString(unit quantityAndUnit) string {
	switch unit {
	case YEAR:
		return "YEAR"
	case MONTH:
		return "MONTH"
	case WEEK:
		return "WEEK"
	case DAY:
		return "DAY"
	case HOUR:
		return "HOUR"
	case MINUTE:
		return "MINUTE"
	case SECOND:
		return "SECOND"
	case MILLISECOND:
		return "MILLISECOND"
	case MICROSECOND:
		return "MICROSECOND"
	case DECADE:
		return "DECADE"
	case CENTURY:
		return "CENTURY"
	case MILLENNIUM:
		return "MILLENNIUM"
	// additional field units for EXTRACT function
	case DOW:
		return "DOW"
	case DOY:
		return "DOY"
	case EPOCH:
		return "EPOCH"
	case ISODOW:
		return "ISODOW"
	case ISOYEAR:
		return "ISOYEAR"
	case JULIAN:
		return "JULIAN"
	case QUARTER:
		return "QUARTER"
	case TIMEZONE:
		return "TIMEZONE"
	case TIMEZONE_HOUR:
		return "TIMEZONE_HOUR"
	case TIMEZONE_MINUTE:
		return "TIMEZONE_MINUTE"
	default:
		panic("jet: invalid INTERVAL unit type")
	}
}

//---------------------------------------------------//
