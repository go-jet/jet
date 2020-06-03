package jet

// SerializeOption type
type SerializeOption int

// Serialize options
const (
	NoWrap SerializeOption = iota
	SkipNewLine

	fallTroughOptions // fall trough options
	ShortName
)

// WithFallTrough extends existing serialize options with additional
func (s SerializeOption) WithFallTrough(options []SerializeOption) []SerializeOption {
	return append(FallTrough(options), s)
}

// StatementType is type of the SQL statement
type StatementType string

// Statement types
const (
	SelectStatementType StatementType = "SELECT"
	InsertStatementType StatementType = "INSERT"
	UpdateStatementType StatementType = "UPDATE"
	DeleteStatementType StatementType = "DELETE"
	SetStatementType    StatementType = "SET"
	LockStatementType   StatementType = "LOCK"
	UnLockStatementType StatementType = "UNLOCK"
	WithStatementType   StatementType = "WITH"
)

// Serializer interface
type Serializer interface {
	serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption)
}

// Serialize func
func Serialize(exp Serializer, statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	exp.serialize(statementType, out, options...)
}

func contains(options []SerializeOption, option SerializeOption) bool {
	for _, opt := range options {
		if opt == option {
			return true
		}
	}

	return false
}

// FallTrough filters fall-trough options from the list
func FallTrough(options []SerializeOption) []SerializeOption {
	var ret []SerializeOption

	for _, option := range options {
		if option > fallTroughOptions {
			ret = append(ret, option)
		}
	}

	return ret
}

// ListSerializer serializes list of serializers with separator
type ListSerializer struct {
	Serializers []Serializer
	Separator   string
}

func (s ListSerializer) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	for i, ser := range s.Serializers {
		if i > 0 {
			out.WriteString(s.Separator)
		}
		ser.serialize(statement, out, FallTrough(options)...)
	}
}

// NewSerializerClauseImpl is constructor for Seralizer with list of clauses
func NewSerializerClauseImpl(clauses ...Clause) Serializer {
	return &serializerImpl{Clauses: clauses}
}

type serializerImpl struct {
	Clauses []Clause
}

func (s serializerImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	for _, clause := range s.Clauses {
		clause.Serialize(statement, out, FallTrough(options)...)
	}
}
