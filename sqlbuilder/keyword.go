package sqlbuilder

const (
	DEFAULT keywordClause = "DEFAULT"
)

type keywordClause string

func (k keywordClause) serialize(out *queryData) error {
	out.WriteString(string(k))

	return nil
}
