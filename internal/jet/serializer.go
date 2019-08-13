package jet

type SerializeOption int

const (
	noWrap SerializeOption = iota
)

type StatementType string

const (
	SelectStatementType StatementType = "SELECT"
	InsertStatementType StatementType = "INSERT"
	UpdateStatementType StatementType = "UPDATE"
	DeleteStatementType StatementType = "DELETE"
	SetStatementType    StatementType = "SET"
	LockStatementType   StatementType = "LOCK"
	UnLockStatementType StatementType = "UNLOCK"
)

type Serializer interface {
	serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption)
}

func Serialize(exp Serializer, statementType StatementType, out *SqlBuilder, options ...SerializeOption) {
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
