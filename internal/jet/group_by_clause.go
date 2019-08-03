package jet

type groupByClause interface {
	serializeForGroupBy(statement StatementType, out *SqlBuilder) error
}
