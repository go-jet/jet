package sqlbuilder

const (
	DEFAULT keywordClause = "DEFAULT"
)

var (
	NULL = newNullExpression()
)

type keywordClause string

func (k keywordClause) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	out.writeString(string(k))

	return nil
}
