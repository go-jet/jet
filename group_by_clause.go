package jet

type groupByClause interface {
	serializeForGroupBy(statement statementType, out *sqlBuilder) error
}
