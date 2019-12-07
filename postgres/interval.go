package postgres

import (
	"fmt"
	"github.com/go-jet/jet/internal/jet"
	"github.com/go-jet/jet/internal/utils"
	"strconv"
	"strings"
	"time"
)

type quantityAndUnit float64

const (
	pow2_32 = -4.294967296e+09

	YEAR quantityAndUnit = pow2_32 + iota
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

type intervalExpressionImpl struct {
	jet.Interval
	jet.ExpressionInterfaceImpl
}

type IntervalExpression interface {
	jet.IsInterval
	jet.Expression
}

func INTERVAL(quantityAndUnit ...quantityAndUnit) IntervalExpression {
	if len(quantityAndUnit)%2 != 0 {
		panic("jet: invalid number of quantity and unit fields")
	}

	fields := []string{}

	for i := 0; i < len(quantityAndUnit); i += 2 {
		quantity := strconv.FormatFloat(float64(quantityAndUnit[i]), 'f', -1, 64)
		unitString := unitToString(quantityAndUnit[i+1])
		fields = append(fields, quantity+" "+unitString)
	}

	intervalStr := fmt.Sprintf("'%s'", strings.Join(fields, " "))

	newInterval := &intervalExpressionImpl{
		Interval: jet.NewInterval(jet.Raw(intervalStr)),
	}

	newInterval.ExpressionInterfaceImpl.Parent = newInterval

	return newInterval
}

func INTERVALd(duration time.Duration) IntervalExpression {
	days, hours, minutes, seconds, microseconds := utils.ExtractDateTimeComponents(duration)

	quantityAndUnits := []quantityAndUnit{}

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
	default:
		panic("jet: invalid INTERVAL unit type")
	}
}

//---------------------------------------------------//

type intervalWrapper struct {
	jet.IsInterval
	Expression
}

func newIntervalExpressionWrap(expression Expression) IntervalExpression {
	intervalWrap := intervalWrapper{Expression: expression}
	return &intervalWrap
}

// IntervalExp is interval expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as interval expression.
// Does not add sql cast to generated sql builder output.
func IntervalExp(expression Expression) IntervalExpression {
	return newIntervalExpressionWrap(expression)
}
