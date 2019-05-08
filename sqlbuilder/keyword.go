package sqlbuilder

const (
	DEFAULT keywordClause = "DEFAULT"
)

type keywordClause string

func (k keywordClause) serialize(statement statementType, out *queryData) error {
	out.writeString(string(k))

	return nil
}
