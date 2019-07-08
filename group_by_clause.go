package jet

type groupByClause interface {
	serializeForGroupBy(statement statementType, out *sqlBuilder) error
}

// TODO: GROUPING SETS, CUBE, and ROLLUP
