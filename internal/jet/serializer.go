package jet

// SerializeOption type
type SerializeOption int

// Serialize options
const (
	noWrap SerializeOption = iota
)

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
