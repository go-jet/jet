package mysql

import "github.com/go-jet/jet/v2/internal/jet"

// Expression is common interface for all expressions.
// Can be Bool, Int, Float, String, Date, Time, Timez, Timestamp or Timestampz expressions.
type Expression = jet.Expression

// BoolExpression interface
type BoolExpression = jet.BoolExpression

// StringExpression interface
type StringExpression = jet.StringExpression

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

// Raw can be used for any unsupported functions, operators or expressions.
// For example: Raw("current_database()")
var Raw = jet.Raw

// Func can be used to call an custom or as of yet unsupported function in the database.
var Func = jet.Func

// NewEnumValue creates new named enum value
var NewEnumValue = jet.NewEnumValue
