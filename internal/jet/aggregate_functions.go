package jet

// AggregateFunc is a named aggregate function (e.g. json_agg) with an optional
// embedded ORDER BY clause. The ORDER BY is serialized inside the function call:
//
//	json_agg(expr ORDER BY col)
//
// which is the PostgreSQL syntax for ordering the input rows of an aggregate.
type AggregateFunc struct {
	Expression

	name     string
	argument Expression
	orderBy  []OrderByClause
}

// ORDER_BY sets the ORDER BY clause of the aggregate input rows.
func (a *AggregateFunc) ORDER_BY(orderBy ...OrderByClause) Expression {
	a.orderBy = orderBy
	return a.Expression
}

// NewAggregateFunc creates a named aggregate function expression. The returned
// Expression carries an optional embedded ORDER BY clause settable via ORDER_BY:
//
//	json_agg(expr ORDER BY col)
func NewAggregateFunc(name string, argument Expression) *AggregateFunc {
	aggFunc := &AggregateFunc{
		name:     name,
		argument: argument,
	}
	aggFunc.Expression = newExpression(newAggregateFuncSerializer(aggFunc))
	return aggFunc
}

func newAggregateFuncSerializer(aggFunc *AggregateFunc) *aggregateFuncSerializer {
	return &aggregateFuncSerializer{AggregateFunc: aggFunc}
}

type aggregateFuncSerializer struct {
	*AggregateFunc
}

func (a *aggregateFuncSerializer) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString(a.name + "(")

	if a.argument != nil {
		wrap(a.argument).serialize(statement, out, FallTrough(options)...)
	}

	if len(a.orderBy) > 0 {
		out.WriteString(" ORDER BY ")
		for i, orderBy := range a.orderBy {
			if i > 0 {
				out.WriteString(", ")
			}
			orderBy.serializeForOrderBy(statement, out)
		}
	}

	out.WriteString(")")
}
