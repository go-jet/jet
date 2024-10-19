package postgres

import "github.com/go-jet/jet/v2/internal/jet"

// Expression is common interface for all expressions.
// Can be Bool, Int, Float, String, Date, Time, Timez, Timestamp or Timestampz expressions.
type Expression = jet.Expression

// BoolExpression interface
type BoolExpression = jet.BoolExpression

// StringExpression interface
type StringExpression = jet.StringExpression

// NumericExpression interface
type NumericExpression = jet.NumericExpression

// IntegerExpression interface
type IntegerExpression = jet.IntegerExpression

// FloatExpression is interface
type FloatExpression = jet.FloatExpression

// TimeExpression interface
type TimeExpression = jet.TimeExpression

// TimezExpression interface for 'time with time zone' types
type TimezExpression = jet.TimezExpression

// DateExpression is interface for date types
type DateExpression = jet.DateExpression

// TimestampExpression interface
type TimestampExpression = jet.TimestampExpression

// TimestampzExpression interface
type TimestampzExpression = jet.TimestampzExpression

// RowExpression interface
type RowExpression = jet.RowExpression

// DateRange Expression interface
type DateRange = jet.Range[DateExpression]

// TimestampRange Expression interface
type TimestampRange = jet.Range[TimestampExpression]

// TimestampzRange Expression interface
type TimestampzRange = jet.Range[TimestampzExpression]

// NumericRange Expression interface
type NumericRange = jet.Range[NumericExpression]

// Int4Range Expression interface
type Int4Range = jet.Range[IntegerExpression]

// Int8Range Expression interface
type Int8Range = jet.Range[IntegerExpression]

// BoolExp is bool expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as bool expression.
// Does not add sql cast to generated sql builder output.
var BoolExp = jet.BoolExp

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

// StringExp is string expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as string expression.
// Does not add sql cast to generated sql builder output.
var StringExp = jet.StringExp

// TimezExp is time with time zone expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as time with time zone expression.
// Does not add sql cast to generated sql builder output.
var TimezExp = jet.TimezExp

// DateExp is date expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as date expression.
// Does not add sql cast to generated sql builder output.
var DateExp = jet.DateExp

// TimestampExp is timestamp expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as timestamp expression.
// Does not add sql cast to generated sql builder output.
var TimestampExp = jet.TimestampExp

// TimestampzExp is timestamp with time zone expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as timestamp with time zone expression.
// Does not add sql cast to generated sql builder output.
var TimestampzExp = jet.TimestampzExp

// RowExp serves as a wrapper for an arbitrary expression, treating it as a row expression.
// This enables the Go compiler to interpret any expression as a row expression
// Note: This does not modify the generated SQL builder output by adding a SQL CAST operation.
var RowExp = jet.RowExp

// RangeExp is range expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as range expression.
// Does not add sql cast to generated sql builder output.
var (
	Int4RangeExp = jet.Int4RangeExp
	Int8RangeExp = jet.Int8RangeExp
	NumRangeExp  = jet.NumRangeExp
	DateRangeExp = jet.DateRangeExp
	TsRangeExp   = jet.TsRangeExp
	TstzRangeExp = jet.TstzRangeExp
)

// CustomExpression is used to define custom expressions.
var CustomExpression = jet.CustomExpression

// Token is used to define custom token in a custom expression.
type Token = jet.Token

// RawArgs is type used to pass optional arguments to Raw method
type RawArgs = map[string]interface{}

// Raw can be used for any unsupported functions, operators or expressions.
// For example: Raw("current_database()")
// Raw helper methods for each of the postgres types
var (
	Raw = jet.Raw

	RawBool            = jet.RawBool
	RawInt             = jet.RawInt
	RawFloat           = jet.RawFloat
	RawString          = jet.RawString
	RawTime            = jet.RawTime
	RawTimez           = jet.RawTimez
	RawTimestamp       = jet.RawTimestamp
	RawTimestampz      = jet.RawTimestampz
	RawDate            = jet.RawDate
	RawNumRange        = jet.RawRange[jet.NumericExpression]
	RawInt4Range       = jet.RawRange[jet.Int4Expression]
	RawInt8Range       = jet.RawRange[jet.Int8Expression]
	RawTimestampRange  = jet.RawRange[jet.TimestampExpression]
	RawTimestampzRange = jet.RawRange[jet.TimestampzExpression]
	RawDateRange       = jet.RawRange[jet.DateExpression]
)

// Func can be used to call custom or unsupported database functions.
var Func = jet.Func

// NewEnumValue creates new named enum value
var NewEnumValue = jet.NewEnumValue

// BinaryOperator can be used to use custom or unsupported operators that take two operands.
var BinaryOperator = jet.BinaryOperator
