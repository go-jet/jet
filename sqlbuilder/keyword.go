package sqlbuilder

const (
	DEFAULT keywordClause = "DEFAULT"
)

type keywordClause string

func (k keywordClause) Serialize(out *queryData, options ...serializeOption) error {
	out.WriteString(string(k))

	return nil
}
