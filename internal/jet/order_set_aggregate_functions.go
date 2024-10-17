package jet

// MODE computes the most frequent value of the aggregated argument
func MODE() *OrderSetAggregateFunc {
	return newOrderSetAggregateFunction("MODE", nil)
}

// PERCENTILE_CONT computes a value corresponding to the specified fraction within the ordered set of
// aggregated argument values. This will interpolate between adjacent input items if needed.
func PERCENTILE_CONT(fraction FloatExpression) *OrderSetAggregateFunc {
	return newOrderSetAggregateFunction("PERCENTILE_CONT", fraction)
}

// PERCENTILE_DISC computes  the first value within the ordered set of aggregated argument values whose position
// in the ordering equals or exceeds the specified fraction. The aggregated argument must be of a sortable type.
func PERCENTILE_DISC(fraction FloatExpression) *OrderSetAggregateFunc {
	return newOrderSetAggregateFunction("PERCENTILE_DISC", fraction)
}

// OrderSetAggregateFunc implementation of order set aggregate function
type OrderSetAggregateFunc struct {
	name     string
	fraction FloatExpression
	orderBy  Window
}

func newOrderSetAggregateFunction(name string, fraction FloatExpression) *OrderSetAggregateFunc {
	return &OrderSetAggregateFunc{
		name:     name,
		fraction: fraction,
	}
}

// WITHIN_GROUP_ORDER_BY specifies ordered set of aggregated argument values
func (p *OrderSetAggregateFunc) WITHIN_GROUP_ORDER_BY(orderBy OrderByClause) Expression {
	p.orderBy = ORDER_BY(orderBy)
	return newOrderSetAggregateFuncExpression(*p)
}

func newOrderSetAggregateFuncExpression(aggFunc OrderSetAggregateFunc) *orderSetAggregateFuncExpression {
	ret := &orderSetAggregateFuncExpression{
		OrderSetAggregateFunc: aggFunc,
	}

	ret.ExpressionInterfaceImpl.Parent = ret

	return ret
}

type orderSetAggregateFuncExpression struct {
	ExpressionInterfaceImpl
	OrderSetAggregateFunc
}

func (p *orderSetAggregateFuncExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString(p.name)

	if p.fraction != nil {
		wrap(p.fraction).serialize(statement, out, FallTrough(options)...)
	} else {
		wrap().serialize(statement, out, FallTrough(options)...)
	}
	out.WriteString("WITHIN GROUP")
	p.orderBy.serialize(statement, out)
}
