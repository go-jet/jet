package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

// Expression is common interface for all expressions.
type Expression = jet.Expression

// BoolExpression interface
type BoolExpression = jet.BoolExpression

// StringExpression interface
type StringExpression = jet.StringExpression

// BlobExpression interface
type BlobExpression = jet.BlobExpression

// IntegerExpression interface
type IntegerExpression = jet.IntegerExpression

// FloatExpression interface
type FloatExpression = jet.FloatExpression

// TimeExpression interface
type TimeExpression = jet.TimeExpression

// DateExpression interface
type DateExpression = jet.DateExpression

// DateTimeExpression interface
type DateTimeExpression = jet.TimestampExpression

// TimestampExpression interface
type TimestampExpression = jet.TimestampExpression

// RowExpression interface
type RowExpression = jet.RowExpression

var (
	BoolExp      = jet.BoolExp
	StringExp    = jet.StringExp
	BlobExp      = jet.BlobExp
	IntExp       = jet.IntExp
	FloatExp     = jet.FloatExp
	TimeExp      = jet.TimeExp
	DateExp      = jet.DateExp
	DateTimeExp  = jet.TimestampExp
	TimestampExp = jet.TimestampExp
	RowExp       = jet.RowExp
)

// CustomExpression is used to define custom expressions without parentheses wrapping.
var CustomExpression = jet.AtomicCustomExpression

// Token is used to define custom token in a custom expression.
type Token = jet.Token

// RawArgs is type used to pass optional arguments to Raw method
type RawArgs = map[string]interface{}

// Raw can be used for any unsupported functions, operators or expressions.
var (
	Raw          = jet.Raw
	RawBool      = jet.RawBool
	RawInt       = jet.RawInt
	RawFloat     = jet.RawFloat
	RawString    = jet.RawString
	RawTime      = jet.RawTime
	RawTimestamp = jet.RawTimestamp
	RawDate      = jet.RawDate
	RawBlob      = jet.RawBlob
)

// Func can be used to call custom or unsupported database functions.
var Func = jet.Func

// NewEnumValue creates new named enum value
var NewEnumValue = jet.NewEnumValue

// BinaryOperator can be used to use custom or unsupported operators.
var BinaryOperator = jet.BinaryOperator
