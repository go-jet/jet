package postgres

import (
	"github.com/go-jet/jet/v2/internal/jet"
)

// Column is common column interface for all types of columns.
type Column = jet.ColumnExpression

// ColumnList function returns list of columns that be used as projection or column list for UPDATE and INSERT statement.
type ColumnList = jet.ColumnList

// ColumnBool is interface for SQL boolean columns.
type ColumnBool = jet.ColumnBool

// BoolColumn creates named bool column.
var BoolColumn = jet.BoolColumn

// ColumnString is interface for SQL text, character, character varying
// bytea, uuid columns and enums types.
type ColumnString = jet.ColumnString

// StringColumn creates named string column.
var StringColumn = jet.StringColumn

// ColumnBytea is interface for bytea columns
type ColumnBytea = jet.ColumnBlob

// ByteaColumn creates new named bytea column.
var ByteaColumn = jet.BlobColumn

// ColumnInteger is interface for SQL smallint, integer, bigint columns.
type ColumnInteger = jet.ColumnInteger

// IntegerColumn creates named integer column.
var IntegerColumn = jet.IntegerColumn

// ColumnFloat is interface for SQL real, numeric, decimal or double precision column.
type ColumnFloat = jet.ColumnFloat

// FloatColumn creates named float column.
var FloatColumn = jet.FloatColumn

// ColumnDate is interface of SQL date columns.
type ColumnDate = jet.ColumnDate

// DateColumn creates named date column.
var DateColumn = jet.DateColumn

// ColumnTime is interface for SQL time column.
type ColumnTime = jet.ColumnTime

// TimeColumn creates named time column
var TimeColumn = jet.TimeColumn

// ColumnTimez is interface of SQL time with time zone columns.
type ColumnTimez = jet.ColumnTimez

// TimezColumn creates named time with time zone column.
var TimezColumn = jet.TimezColumn

// ColumnTimestamp is interface of SQL timestamp columns.
type ColumnTimestamp = jet.ColumnTimestamp

// TimestampColumn creates named timestamp column
var TimestampColumn = jet.TimestampColumn

// ColumnTimestampz is interface of SQL timestamp with timezone columns.
type ColumnTimestampz = jet.ColumnTimestampz

// TimestampzColumn creates named timestamp with time zone column.
var TimestampzColumn = jet.TimestampzColumn

// ColumnInterval is interface of PostgreSQL interval columns.
type ColumnInterval = jet.ColumnInterval

// IntervalColumn creates named interval column
var IntervalColumn = jet.IntervalColumn

// ColumnDateRange is interface of SQL date range column
type ColumnDateRange = jet.ColumnRange[DateExpression]

// DateRangeColumn creates named range with range column
var DateRangeColumn = jet.RangeColumn[DateExpression]

// ColumnNumericRange is interface of SQL numeric range column
type ColumnNumericRange = jet.ColumnRange[NumericExpression]

// NumericRangeColumn creates named range with range column
var NumericRangeColumn = jet.RangeColumn[NumericExpression]

// ColumnTimestampRange is interface of SQL timestamp range column
type ColumnTimestampRange = jet.ColumnRange[TimestampExpression]

// TimestampRangeColumn creates named range with range column
var TimestampRangeColumn = jet.RangeColumn[TimestampExpression]

// ColumnTimestampzRange is interface of SQL timestamp range column
type ColumnTimestampzRange = jet.ColumnRange[TimestampzExpression]

// TimestampzRangeColumn creates named range with range column
var TimestampzRangeColumn = jet.RangeColumn[TimestampzExpression]

// ColumnInt4Range is interface of SQL int4 range column
type ColumnInt4Range jet.ColumnRange[jet.Int4Expression]

// Int4RangeColumn creates named range with range column
var Int4RangeColumn = jet.RangeColumn[jet.Int4Expression]

// ColumnInt8Range is interface of SQL int8 range column
type ColumnInt8Range jet.ColumnRange[jet.Int8Expression]

// Int8RangeColumn creates named range with range column
var Int8RangeColumn = jet.RangeColumn[jet.Int8Expression]

// ColumnStringArray is interface of column
type ColumnStringArray jet.ColumnArray[jet.StringExpression]

// StringArrayColumn creates named string array column
var StringArrayColumn = jet.ArrayColumn[jet.StringExpression]

// ColumnIntegerArray is interface of column
type ColumnIntegerArray jet.ColumnArray[jet.IntegerExpression]

// IntegerArrayColumn creates named integer array column
var IntegerArrayColumn = jet.ArrayColumn[jet.IntegerExpression]

// ColumnBoolArray is interface of column
type ColumnBoolArray jet.ColumnArray[jet.BoolExpression]

// BoolArrayColumn creates named bool array column
var BoolArrayColumn = jet.ArrayColumn[jet.BoolExpression]
