package jet

// StringOrBlobExpression is common interface for all string and blob expressions
type StringOrBlobExpression interface {
	Expression

	isStringOrBlob()
}
