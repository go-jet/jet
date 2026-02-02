package jet

import "slices"

// SerializeOption type
type SerializeOption int

// Serialize options
const (
	NoWrap SerializeOption = iota
	SkipNewLine
	Ident

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
	SelectStatementType        StatementType = "SELECT"
	SelectJsonObjStatementType StatementType = "SELECT_JSON_OBJ"
	SelectJsonArrStatementType StatementType = "SELECT_JSON_ARR"
	InsertStatementType        StatementType = "INSERT"
	UpdateStatementType        StatementType = "UPDATE"
	DeleteStatementType        StatementType = "DELETE"
	SetStatementType           StatementType = "SET"
	LockStatementType          StatementType = "LOCK"
	UnLockStatementType        StatementType = "UNLOCK"
	WithStatementType          StatementType = "WITH"
)

// Serializer interface
type Serializer interface {
	serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption)
}

// Serialize func
func Serialize(exp Serializer, statementType StatementType, out *SQLBuilder, options ...SerializeOption) {
	exp.serialize(statementType, out, options...)
}

func SerializeForOrderBy(exp Expression, statementType StatementType, out *SQLBuilder) {
	exp.serializeForOrderBy(statementType, out)
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

func without(options []SerializeOption, option SerializeOption) []SerializeOption {
	return slices.DeleteFunc(options, func(elem SerializeOption) bool {
		return elem == option
	})
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

// Token can be used to construct complex custom expressions
type Token string

func (t Token) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString(string(t))
}

// CustomExpression creates new custom expression. When serialized may require parentheses
// depending on context.
func CustomExpression(parts ...Serializer) Expression {
	return newExpression(&customSerializer{
		parts: parts,
	})
}

// AtomicCustomExpression creates new custom expression. When serialized does not require parentheses.
func AtomicCustomExpression(parts ...Serializer) Expression {
	return newExpression(&customSerializer{
		parts:  parts,
		atomic: true,
	})
}

type customSerializer struct {
	parts  []Serializer
	atomic bool
}

func (c *customSerializer) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if c.atomic {
		for _, expr := range c.parts {
			expr.serialize(statement, out, without(options, NoWrap)...)
		}
	} else {
		optionalWrap(out, options, func(out *SQLBuilder, options []SerializeOption) {
			for _, expr := range c.parts {
				expr.serialize(statement, out, options...)
			}
		})
	}
}

func optionalWrap(out *SQLBuilder, options []SerializeOption, ser func(out *SQLBuilder, options []SerializeOption)) {
	if !contains(options, NoWrap) {
		out.WriteString("(")
	}

	ser(out, without(options, NoWrap))

	if !contains(options, NoWrap) {
		out.WriteString(")")
	}
}

func wrap(expressions ...Expression) Expression {
	return newFunc("", expressions)
}
