package sqlbuilder

type projection interface {
	serializeForProjection(statement statementType, out *queryData) error
}
