package jet

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCastAS(t *testing.T) {
	assertClauseSerialize(t, NewCastImpl(Int(1)).AS("boolean"), "CAST($1 AS boolean)", int64(1))
	assertClauseSerialize(t, NewCastImpl(table2Col3).AS("real"), "CAST(table2.col3 AS real)")
	assertClauseSerialize(t, NewCastImpl(table2Col3.ADD(table2Col3)).AS("integer"), "CAST((table2.col3 + table2.col3) AS integer)")
}

func TestCastAS_WithDialectOverride(t *testing.T) {
	dialectWithOverride := NewDialect(DialectParams{
		AliasQuoteChar:      '"',
		IdentifierQuoteChar: '"',
		ArgumentPlaceholder: func(ord int) string {
			return "?" + strconv.Itoa(ord)
		},
		ArgumentToString: func(value any) (string, bool) {
			return "", false
		},
		OperatorSerializeOverrides: map[string]SerializeOverride{
			"CAST": func(expressions ...Serializer) SerializerFunc {
				return func(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
					out.WriteString("CUSTOM_CAST(")
					expressions[0].serialize(statement, out, FallTrough(options)...)
					out.WriteString("AS")
					expressions[1].serialize(statement, out, FallTrough(options)...)
					out.WriteString(")")
				}
			},
		},
	})

	castExpr := NewCastImpl(Int(1)).AS("INTEGER")
	out := SQLBuilder{Dialect: dialectWithOverride}
	castExpr.serialize(SelectStatementType, &out)

	require.Contains(t, out.Buff.String(), "CUSTOM_CAST(")
}
