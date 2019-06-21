package jet

type groupByClause interface {
	serializeForGroupBy(statement statementType, out *queryData) error
}

// TODO: GROUPING SETS, CUBE, and ROLLUP
