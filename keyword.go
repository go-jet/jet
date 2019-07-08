package jet

const (
	DEFAULT keywordClause = "DEFAULT"
)

var (
	NULL = newNullLiteral()
	STAR = newStarLiteral()
)

type keywordClause string

func (k keywordClause) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	out.writeString(string(k))

	return nil
}
