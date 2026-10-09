package postgres

import "github.com/go-jet/jet/v2/internal/jet"

// JSON_AGG returns a json_agg aggregate expression. Aggregate input rows can be
// ordered with ORDER_BY, e.g.:
//
//	JSON_AGG(expr).ORDER_BY(Column.ASC())
//
// which serializes to json_agg(expr ORDER BY column ASC).
func JSON_AGG(expression Expression) *jet.AggregateFunc {
	return jet.NewAggregateFunc("json_agg", expression)
}

// JSONB_AGG returns a jsonb_agg aggregate expression with optional ORDER BY.
func JSONB_AGG(expression Expression) *jet.AggregateFunc {
	return jet.NewAggregateFunc("jsonb_agg", expression)
}

// ARRAY_AGG returns an array_agg aggregate expression with optional ORDER BY.
func ARRAY_AGG(expression Expression) *jet.AggregateFunc {
	return jet.NewAggregateFunc("array_agg", expression)
}
