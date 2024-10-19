package sqlite

import "github.com/go-jet/jet/v2/internal/jet"

// Expression is common interface for all expressions.
// Can be Bool, Int, Float, String, Date, Time or Timestamp expressions.
type Expression = jet.Expression

// BoolExpression interface
type BoolExpression = jet.BoolExpression

// StringExpression interface
type StringExpression = jet.StringExpression

// NumericExpression is shared interface for integer or real expression
type NumericExpression = jet.NumericExpression

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

// BoolExp is bool expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as bool expression.
// Does not add sql cast to generated sql builder output.
var BoolExp = jet.BoolExp

// StringExp is string expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as string expression.
// Does not add sql cast to generated sql builder output.
var StringExp = jet.StringExp

// IntExp is int expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as int expression.
// Does not add sql cast to generated sql builder output.
var IntExp = jet.IntExp

// FloatExp is date expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as float expression.
// Does not add sql cast to generated sql builder output.
var FloatExp = jet.FloatExp

// TimeExp is time expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as time expression.
// Does not add sql cast to generated sql builder output.
var TimeExp = jet.TimeExp

// DateExp is date expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as date expression.
// Does not add sql cast to generated sql builder output.
var DateExp = jet.DateExp

// DateTimeExp is timestamp expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as timestamp expression.
// Does not add sql cast to generated sql builder output.
var DateTimeExp = jet.TimestampExp

// TimestampExp is timestamp expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as timestamp expression.
// Does not add sql cast to generated sql builder output.
var TimestampExp = jet.TimestampExp

// RowExp serves as a wrapper for an arbitrary expression, treating it as a row expression.
// This enables the Go compiler to interpret any expression as a row expression
// Note: This does not modify the generated SQL builder output by adding a SQL CAST operation.
var RowExp = jet.RowExp

// CustomExpression is used to define custom expressions.
var CustomExpression = jet.CustomExpression

// Token is used to define custom token in a custom expression.
type Token = jet.Token

// RawArgs is type used to pass optional arguments to Raw method
type RawArgs = map[string]interface{}

// Raw can be used for any unsupported functions, operators or expressions.
// For example: Raw("current_database()")
// Raw helper methods for each of the sqlite types
var (
	Raw = jet.Raw

	RawBool      = jet.RawBool
	RawInt       = jet.RawInt
	RawFloat     = jet.RawFloat
	RawString    = jet.RawString
	RawTime      = jet.RawTime
	RawTimestamp = jet.RawTimestamp
	RawDate      = jet.RawDate
)

// Func can be used to call custom or unsupported database functions.
var Func = jet.Func

// NewEnumValue creates new named enum value
var NewEnumValue = jet.NewEnumValue

// BinaryOperator can be used to use custom or unsupported operators that take two operands.
var BinaryOperator = jet.BinaryOperator
