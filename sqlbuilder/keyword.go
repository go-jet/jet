package sqlbuilder

import "bytes"

const (
	DEFAULT keywordClause = "DEFAULT"
)

type keywordClause string

func (k keywordClause) SerializeSql(out *bytes.Buffer, options ...serializeOption) error {
	out.WriteString(string(k))

	return nil
}
