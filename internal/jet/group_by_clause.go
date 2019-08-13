package jet

type GroupByClause interface {
	serializeForGroupBy(statement StatementType, out *SqlBuilder)
}
