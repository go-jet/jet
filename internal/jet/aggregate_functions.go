package jet

func MODE() *OrderSetAggregateFunc {
	return newOrderSetAggregateFunction("MODE", nil)
}

func PERCENTILE_CONT(fraction FloatExpression) *OrderSetAggregateFunc {
	return newOrderSetAggregateFunction("PERCENTILE_CONT", fraction)
}

func PERCENTILE_DISC(fraction FloatExpression) *OrderSetAggregateFunc {
	return newOrderSetAggregateFunction("PERCENTILE_DISC", fraction)
}

type OrderSetAggregateFunc struct {
	name     string
	fraction FloatExpression
	orderBy  Window
}

func newOrderSetAggregateFunction(name string, fraction FloatExpression) *OrderSetAggregateFunc {
	ret := &OrderSetAggregateFunc{
		name:     name,
		fraction: fraction,
	}

	return ret
}

func (p *OrderSetAggregateFunc) WITHIN_GROUP(orderBy Window) Expression {
	p.orderBy = orderBy
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
	WRAP(p.fraction).serialize(statement, out, options...)
	out.WriteString("WITHIN GROUP")
	p.orderBy.serialize(statement, out)
}
