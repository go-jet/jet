package sqlbuilder

type groupByClause interface {
	serializeForGroupBy(out *queryData) error
}

// TODO: GROUPING SETS, CUBE, and ROLLUP
