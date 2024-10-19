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

//------------------------------------------------------//

// ColumnInterval is interface of PostgreSQL interval columns.
type ColumnInterval interface {
	IntervalExpression
	jet.Column

	From(subQuery SelectTable) ColumnInterval
	SET(intervalExp IntervalExpression) ColumnAssigment
}

//------------------------------------------------------//

type intervalColumnImpl struct {
	jet.ColumnExpressionImpl
	intervalInterfaceImpl
}

func (i *intervalColumnImpl) SET(intervalExp IntervalExpression) ColumnAssigment {
	return jet.NewColumnAssignment(i, intervalExp)
}

func (i *intervalColumnImpl) From(subQuery SelectTable) ColumnInterval {
	newIntervalColumn := IntervalColumn(i.Name())
	jet.SetTableName(newIntervalColumn, i.TableName())
	jet.SetSubQuery(newIntervalColumn, subQuery)

	return newIntervalColumn
}

// IntervalColumn creates named interval column.
func IntervalColumn(name string) ColumnInterval {
	intervalColumn := &intervalColumnImpl{}
	intervalColumn.ColumnExpressionImpl = jet.NewColumnImpl(name, "", intervalColumn)
	intervalColumn.intervalInterfaceImpl.parent = intervalColumn
	return intervalColumn
}
