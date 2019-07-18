package jet

const (
	// DEFAULT is jet equivalent of SQL DEFAULT
	DEFAULT keywordClause = "DEFAULT"
)

var (
	// NULL is jet equivalent of SQL NULL
	NULL = newNullLiteral()
	// STAR is jet equivalent of SQL *
	STAR = newStarLiteral()
)

type keywordClause string

func (k keywordClause) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	out.writeString(string(k))

	return nil
}
