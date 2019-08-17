package jet

// GroupByClause interface
type GroupByClause interface {
	serializeForGroupBy(statement StatementType, out *SQLBuilder)
}
