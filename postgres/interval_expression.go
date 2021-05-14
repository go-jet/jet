package postgres

import (
	"fmt"
	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/internal/utils"
	"strconv"
	"strings"
	"time"
)

type quantityAndUnit = float64

// Interval unit types
const (
	YEAR quantityAndUnit = 123456789 + iota
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

// IntervalExpression is representation of postgres INTERVAL
type IntervalExpression interface {
	jet.IsInterval
	jet.Expression

	EQ(rhs IntervalExpression) BoolExpression
	NOT_EQ(rhs IntervalExpression) BoolExpression
	IS_DISTINCT_FROM(rhs IntervalExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs IntervalExpression) BoolExpression

	LT(rhs IntervalExpression) BoolExpression
	LT_EQ(rhs IntervalExpression) BoolExpression
	GT(rhs IntervalExpression) BoolExpression
	GT_EQ(rhs IntervalExpression) BoolExpression

	ADD(rhs IntervalExpression) IntervalExpression
	SUB(rhs IntervalExpression) IntervalExpression

	MUL(rhs NumericExpression) IntervalExpression
	DIV(rhs NumericExpression) IntervalExpression
}

type intervalInterfaceImpl struct {
	jet.IsIntervalImpl

	parent IntervalExpression
}

func (i *intervalInterfaceImpl) EQ(rhs IntervalExpression) BoolExpression {
	return jet.Eq(i.parent, rhs)
}

func (i *intervalInterfaceImpl) NOT_EQ(rhs IntervalExpression) BoolExpression {
	return jet.NotEq(i.parent, rhs)
}

func (i *intervalInterfaceImpl) IS_DISTINCT_FROM(rhs IntervalExpression) BoolExpression {
	return jet.IsDistinctFrom(i.parent, rhs)
}

func (i *intervalInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs IntervalExpression) BoolExpression {
	return jet.IsNotDistinctFrom(i.parent, rhs)
}

func (i *intervalInterfaceImpl) LT(rhs IntervalExpression) BoolExpression {
	return jet.Lt(i.parent, rhs)
}

func (i *intervalInterfaceImpl) LT_EQ(rhs IntervalExpression) BoolExpression {
	return jet.LtEq(i.parent, rhs)
}

func (i *intervalInterfaceImpl) GT(rhs IntervalExpression) BoolExpression {
	return jet.Gt(i.parent, rhs)
}

func (i *intervalInterfaceImpl) GT_EQ(rhs IntervalExpression) BoolExpression {
	return jet.GtEq(i.parent, rhs)
}

func (i *intervalInterfaceImpl) ADD(rhs IntervalExpression) IntervalExpression {
	return IntervalExp(jet.Add(i.parent, rhs))
}

func (i *intervalInterfaceImpl) SUB(rhs IntervalExpression) IntervalExpression {
	return IntervalExp(jet.Sub(i.parent, rhs))
}

func (i *intervalInterfaceImpl) MUL(rhs NumericExpression) IntervalExpression {
	return IntervalExp(jet.Mul(i.parent, rhs))
}

func (i *intervalInterfaceImpl) DIV(rhs NumericExpression) IntervalExpression {
	return IntervalExp(jet.Div(i.parent, rhs))
}

type intervalExpression struct {
	jet.Expression
	intervalInterfaceImpl
}

// INTERVAL creates new interval expression from the list of quantity-unit pairs.
// For example: INTERVAL(1, DAY, 3, MINUTE)
func INTERVAL(quantityAndUnit ...quantityAndUnit) IntervalExpression {
	quantityAndUnitLen := len(quantityAndUnit)
	if quantityAndUnitLen == 0 || quantityAndUnitLen%2 != 0 {
		panic("jet: invalid number of quantity and unit fields")
	}

	fields := []string{}

	for i := 0; i < len(quantityAndUnit); i += 2 {
		quantity := strconv.FormatFloat(quantityAndUnit[i], 'f', -1, 64)
		unitString := unitToString(quantityAndUnit[i+1])
		fields = append(fields, quantity+" "+unitString)
	}

	intervalStr := fmt.Sprintf("INTERVAL '%s'", strings.Join(fields, " "))

	newInterval := &intervalExpression{}

	newInterval.Expression = jet.RawWithParent(intervalStr, newInterval)
	newInterval.intervalInterfaceImpl.parent = newInterval

	return newInterval
}

// INTERVALd creates interval expression from time.Duration
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
	intervalInterfaceImpl
	Expression
}

func newIntervalExpressionWrap(expression Expression) IntervalExpression {
	intervalWrap := &intervalWrapper{Expression: expression}
	intervalWrap.intervalInterfaceImpl.parent = intervalWrap
	return intervalWrap
}

// IntervalExp is interval expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as interval expression.
// Does not add sql cast to generated sql builder output.
func IntervalExp(expression Expression) IntervalExpression {
	return newIntervalExpressionWrap(expression)
}
