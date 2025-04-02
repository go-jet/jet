package jet

type rawStatementImpl struct {
	statementInterfaceImpl

	RawQuery       string
	NamedArguments map[string]interface{}
}

// RawStatement creates new sql statements from raw query and optional map of named arguments
func RawStatement(dialect Dialect, rawQuery string, namedArgument ...map[string]interface{}) SerializerStatement {
	newRawStatement := rawStatementImpl{
		statementInterfaceImpl: statementInterfaceImpl{
			dialect:       dialect,
			statementType: "",
			root:          nil,
		},
		RawQuery: rawQuery,
	}

	if len(namedArgument) > 0 {
		newRawStatement.NamedArguments = namedArgument[0]
	}

	newRawStatement.root = &newRawStatement

	return &newRawStatement
}

func (s *rawStatementImpl) projections() ProjectionList {
	return nil
}

func (s *rawStatementImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if !contains(options, NoWrap) {
		out.WriteString("(")
		out.IncreaseIdent()
	}

	out.insertRawQuery(s.RawQuery, s.NamedArguments)

	if !contains(options, NoWrap) {
		out.DecreaseIdent()
		out.NewLine()
		out.WriteString(")")
	}
}
