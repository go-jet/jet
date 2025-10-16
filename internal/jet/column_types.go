package jet

// ColumnBool is interface for SQL boolean columns.
type ColumnBool interface {
	BoolExpression
	Column

	From(subQuery SelectTable) ColumnBool
	SET(boolExp BoolExpression) ColumnAssigment
}

type boolColumnImpl struct {
	boolInterfaceImpl
	*ColumnExpressionImpl
}

func (i *boolColumnImpl) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *boolColumnImpl) From(subQuery SelectTable) ColumnBool {
	newBoolColumn := BoolColumn(i.name)
	newBoolColumn.setTableName(i.tableName)
	newBoolColumn.setSubQuery(subQuery)

	return newBoolColumn
}

func (i *boolColumnImpl) SET(boolExp BoolExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: boolExp,
	}
}

// BoolColumn creates named bool column.
func BoolColumn(name string) ColumnBool {
	boolColumn := &boolColumnImpl{}
	boolColumn.ColumnExpressionImpl = NewColumnImpl(name, "", boolColumn)
	boolColumn.boolInterfaceImpl.root = boolColumn

	return boolColumn
}

//------------------------------------------------------//

// ColumnFloat is interface for SQL real, numeric, decimal or double precision column.
type ColumnFloat interface {
	FloatExpression
	Column

	From(subQuery SelectTable) ColumnFloat
	SET(floatExp FloatExpression) ColumnAssigment
}

type floatColumnImpl struct {
	floatInterfaceImpl
	*ColumnExpressionImpl
}

func (i *floatColumnImpl) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *floatColumnImpl) From(subQuery SelectTable) ColumnFloat {
	newFloatColumn := FloatColumn(i.name)
	newFloatColumn.setTableName(i.tableName)
	newFloatColumn.setSubQuery(subQuery)

	return newFloatColumn
}

func (i *floatColumnImpl) SET(floatExp FloatExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: floatExp,
	}
}

// FloatColumn creates named float column.
func FloatColumn(name string) ColumnFloat {
	floatColumn := &floatColumnImpl{}
	floatColumn.floatInterfaceImpl.root = floatColumn
	floatColumn.ColumnExpressionImpl = NewColumnImpl(name, "", floatColumn)

	return floatColumn
}

//------------------------------------------------------//

// ColumnInteger is interface for SQL smallint, integer, bigint columns.
type ColumnInteger interface {
	IntegerExpression
	Column

	From(subQuery SelectTable) ColumnInteger
	SET(intExp IntegerExpression) ColumnAssigment
}

type integerColumnImpl struct {
	integerInterfaceImpl

	*ColumnExpressionImpl
}

func (i *integerColumnImpl) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *integerColumnImpl) From(subQuery SelectTable) ColumnInteger {
	newIntColumn := IntegerColumn(i.name)
	newIntColumn.setTableName(i.tableName)
	newIntColumn.setSubQuery(subQuery)

	return newIntColumn
}

func (i *integerColumnImpl) SET(intExp IntegerExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: intExp,
	}
}

// IntegerColumn creates named integer column.
func IntegerColumn(name string) ColumnInteger {
	integerColumn := &integerColumnImpl{}
	integerColumn.integerInterfaceImpl.root = integerColumn
	integerColumn.ColumnExpressionImpl = NewColumnImpl(name, "", integerColumn)

	return integerColumn
}

//------------------------------------------------------//

type ColumnArray[E Expression] interface {
	ArrayExpression[E]
	Column

	From(subQuery SelectTable) ColumnArray[E]
	SET(stringExp ArrayExpression[E]) ColumnAssigment
}

type arrayColumnImpl[E Expression] struct {
	arrayInterfaceImpl[E]

	ColumnExpressionImpl
}

func (a arrayColumnImpl[E]) From(subQuery SelectTable) ColumnArray[E] {
	newArrayColumn := ArrayColumn[E](a.name)
	newArrayColumn.setTableName(a.tableName)
	newArrayColumn.setSubQuery(subQuery)

	return newArrayColumn
}

func (a *arrayColumnImpl[E]) SET(stringExp ArrayExpression[E]) ColumnAssigment {
	return columnAssigmentImpl{
		column:     a,
		expression: stringExp,
	}
}

// StringColumn creates named string column.
func ArrayColumn[E Expression](name string) ColumnArray[E] {
	arrayColumn := &arrayColumnImpl[E]{}
	arrayColumn.arrayInterfaceImpl.parent = arrayColumn
	arrayColumn.ColumnExpressionImpl = NewColumnImpl(name, "", arrayColumn)

	return arrayColumn
}

//------------------------------------------------------//

// ColumnString is interface for SQL text, character, character varying
// uuid columns and enums types.
type ColumnString interface {
	StringExpression
	Column

	From(subQuery SelectTable) ColumnString
	SET(stringExp StringExpression) ColumnAssigment
}

type stringColumnImpl struct {
	stringInterfaceImpl

	*ColumnExpressionImpl
}

func (i *stringColumnImpl) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *stringColumnImpl) From(subQuery SelectTable) ColumnString {
	newStrColumn := StringColumn(i.name)
	newStrColumn.setTableName(i.tableName)
	newStrColumn.setSubQuery(subQuery)

	return newStrColumn
}

func (i *stringColumnImpl) SET(stringExp StringExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: stringExp,
	}
}

// StringColumn creates named string column.
func StringColumn(name string) ColumnString {
	stringColumn := &stringColumnImpl{}
	stringColumn.stringInterfaceImpl.root = stringColumn
	stringColumn.ColumnExpressionImpl = NewColumnImpl(name, "", stringColumn)

	return stringColumn
}

//------------------------------------------------------//

// ColumnBlob is interface for binary data types (bytea, binary, blob, etc...)
type ColumnBlob interface {
	BlobExpression
	Column

	From(subQuery SelectTable) ColumnBlob
	SET(blob BlobExpression) ColumnAssigment
}

type blobColumnImpl struct {
	blobInterfaceImpl

	*ColumnExpressionImpl
}

func (i *blobColumnImpl) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *blobColumnImpl) From(subQuery SelectTable) ColumnBlob {
	newBlobColumn := BlobColumn(i.name)
	newBlobColumn.setTableName(i.tableName)
	newBlobColumn.setSubQuery(subQuery)

	return newBlobColumn
}

func (i *blobColumnImpl) SET(blobExp BlobExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: blobExp,
	}
}

// BlobColumn creates named blob column.
func BlobColumn(name string) ColumnBlob {
	blobColumn := &blobColumnImpl{}
	blobColumn.blobInterfaceImpl.root = blobColumn
	blobColumn.ColumnExpressionImpl = NewColumnImpl(name, "", blobColumn)

	return blobColumn
}

//------------------------------------------------------//

// ColumnTime is interface for SQL time column.
type ColumnTime interface {
	TimeExpression
	Column

	From(subQuery SelectTable) ColumnTime
	SET(timeExp TimeExpression) ColumnAssigment
}

type timeColumnImpl struct {
	timeInterfaceImpl
	*ColumnExpressionImpl
}

func (i *timeColumnImpl) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *timeColumnImpl) From(subQuery SelectTable) ColumnTime {
	newTimeColumn := TimeColumn(i.name)
	newTimeColumn.setTableName(i.tableName)
	newTimeColumn.setSubQuery(subQuery)

	return newTimeColumn
}

func (i *timeColumnImpl) SET(timeExp TimeExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: timeExp,
	}
}

// TimeColumn creates named time column
func TimeColumn(name string) ColumnTime {
	timeColumn := &timeColumnImpl{}
	timeColumn.timeInterfaceImpl.root = timeColumn
	timeColumn.ColumnExpressionImpl = NewColumnImpl(name, "", timeColumn)
	return timeColumn
}

//------------------------------------------------------//

// ColumnTimez is interface of SQL time with time zone columns.
type ColumnTimez interface {
	TimezExpression
	Column

	From(subQuery SelectTable) ColumnTimez
	SET(timeExp TimezExpression) ColumnAssigment
}

type timezColumnImpl struct {
	timezInterfaceImpl
	*ColumnExpressionImpl
}

func (i *timezColumnImpl) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *timezColumnImpl) From(subQuery SelectTable) ColumnTimez {
	newTimezColumn := TimezColumn(i.name)
	newTimezColumn.setTableName(i.tableName)
	newTimezColumn.setSubQuery(subQuery)

	return newTimezColumn
}

func (i *timezColumnImpl) SET(timezExp TimezExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: timezExp,
	}
}

// TimezColumn creates named time with time zone column.
func TimezColumn(name string) ColumnTimez {
	timezColumn := &timezColumnImpl{}
	timezColumn.timezInterfaceImpl.root = timezColumn
	timezColumn.ColumnExpressionImpl = NewColumnImpl(name, "", timezColumn)

	return timezColumn
}

//------------------------------------------------------//

// ColumnTimestamp is interface of SQL timestamp columns.
type ColumnTimestamp interface {
	TimestampExpression
	Column

	From(subQuery SelectTable) ColumnTimestamp
	SET(timestampExp TimestampExpression) ColumnAssigment
}

type timestampColumnImpl struct {
	timestampInterfaceImpl
	*ColumnExpressionImpl
}

func (i *timestampColumnImpl) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *timestampColumnImpl) From(subQuery SelectTable) ColumnTimestamp {
	newTimestampColumn := TimestampColumn(i.name)
	newTimestampColumn.setTableName(i.tableName)
	newTimestampColumn.setSubQuery(subQuery)

	return newTimestampColumn
}

func (i *timestampColumnImpl) SET(timestampExp TimestampExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: timestampExp,
	}
}

// TimestampColumn creates named timestamp column
func TimestampColumn(name string) ColumnTimestamp {
	timestampColumn := &timestampColumnImpl{}
	timestampColumn.timestampInterfaceImpl.root = timestampColumn
	timestampColumn.ColumnExpressionImpl = NewColumnImpl(name, "", timestampColumn)

	return timestampColumn
}

//------------------------------------------------------//

// ColumnTimestampz is interface of SQL timestamp with timezone columns.
type ColumnTimestampz interface {
	TimestampzExpression
	Column

	From(subQuery SelectTable) ColumnTimestampz
	SET(timestampzExp TimestampzExpression) ColumnAssigment
}

type timestampzColumnImpl struct {
	timestampzInterfaceImpl
	*ColumnExpressionImpl
}

func (i *timestampzColumnImpl) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *timestampzColumnImpl) From(subQuery SelectTable) ColumnTimestampz {
	newTimestampzColumn := TimestampzColumn(i.name)
	newTimestampzColumn.setTableName(i.tableName)
	newTimestampzColumn.setSubQuery(subQuery)

	return newTimestampzColumn
}

func (i *timestampzColumnImpl) SET(timestampzExp TimestampzExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: timestampzExp,
	}
}

// TimestampzColumn creates named timestamp with time zone column.
func TimestampzColumn(name string) ColumnTimestampz {
	timestampzColumn := &timestampzColumnImpl{}
	timestampzColumn.timestampzInterfaceImpl.root = timestampzColumn
	timestampzColumn.ColumnExpressionImpl = NewColumnImpl(name, "", timestampzColumn)

	return timestampzColumn
}

//------------------------------------------------------//

// ColumnDate is interface of SQL date columns.
type ColumnDate interface {
	DateExpression
	Column

	From(subQuery SelectTable) ColumnDate
	SET(dateExp DateExpression) ColumnAssigment
}

type dateColumnImpl struct {
	dateInterfaceImpl
	*ColumnExpressionImpl
}

func (i *dateColumnImpl) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *dateColumnImpl) From(subQuery SelectTable) ColumnDate {
	newDateColumn := DateColumn(i.name)
	newDateColumn.setTableName(i.tableName)
	newDateColumn.setSubQuery(subQuery)

	return newDateColumn
}

func (i *dateColumnImpl) SET(dateExp DateExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: dateExp,
	}
}

// DateColumn creates named date column.
func DateColumn(name string) ColumnDate {
	dateColumn := &dateColumnImpl{}
	dateColumn.dateInterfaceImpl.root = dateColumn
	dateColumn.ColumnExpressionImpl = NewColumnImpl(name, "", dateColumn)
	return dateColumn
}

//------------------------------------------------------//

// ColumnInterval is interface of PostgreSQL interval columns.
type ColumnInterval interface {
	IntervalExpression
	Column

	From(subQuery SelectTable) ColumnInterval
	SET(intervalExp IntervalExpression) ColumnAssigment
}

//------------------------------------------------------//

type intervalColumnImpl struct {
	*ColumnExpressionImpl
	intervalInterfaceImpl
}

func (i *intervalColumnImpl) SET(intervalExp IntervalExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: intervalExp,
	}
}

func (i *intervalColumnImpl) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *intervalColumnImpl) From(subQuery SelectTable) ColumnInterval {
	newIntervalColumn := IntervalColumn(i.name)
	newIntervalColumn.setTableName(i.tableName)
	newIntervalColumn.setSubQuery(subQuery)

	return newIntervalColumn
}

// IntervalColumn creates named interval column.
func IntervalColumn(name string) ColumnInterval {
	intervalColumn := &intervalColumnImpl{}
	intervalColumn.ColumnExpressionImpl = NewColumnImpl(name, "", intervalColumn)
	intervalColumn.intervalInterfaceImpl.root = intervalColumn
	return intervalColumn
}

//------------------------------------------------------//

// ColumnRange is interface for range columns which can be int range, string range
// timestamp range or date range.
type ColumnRange[T Expression] interface {
	Range[T]
	Column

	From(subQuery SelectTable) ColumnRange[T]
	SET(rangeExp Range[T]) ColumnAssigment
}

type rangeColumnImpl[T Expression] struct {
	rangeInterfaceImpl[T]
	*ColumnExpressionImpl
}

func (i *rangeColumnImpl[T]) fromImpl(subQuery SelectTable) Projection {
	return i.From(subQuery)
}

func (i *rangeColumnImpl[T]) From(subQuery SelectTable) ColumnRange[T] {
	newRangeColumn := RangeColumn[T](i.name)
	newRangeColumn.setTableName(i.tableName)
	newRangeColumn.setSubQuery(subQuery)

	return newRangeColumn
}

func (i *rangeColumnImpl[T]) SET(rangeExp Range[T]) ColumnAssigment {
	return columnAssigmentImpl{
		column:   i,
		toAssign: rangeExp,
	}
}

// RangeColumn creates named range column.
func RangeColumn[T Expression](name string) ColumnRange[T] {
	rangeColumn := &rangeColumnImpl[T]{}
	rangeColumn.rangeInterfaceImpl.root = rangeColumn
	rangeColumn.ColumnExpressionImpl = NewColumnImpl(name, "", rangeColumn)

	return rangeColumn
}
