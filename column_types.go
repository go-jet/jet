package jet

// ColumnBool is interface for SQL boolean columns.
type ColumnBool interface {
	BoolExpression
	column

	From(subQuery SelectTable) ColumnBool
}

type boolColumnImpl struct {
	boolInterfaceImpl

	columnImpl
}

func (i *boolColumnImpl) from(subQuery SelectTable) projection {
	newBoolColumn := BoolColumn(i.name)
	newBoolColumn.setTableName(i.tableName)
	newBoolColumn.setSubQuery(subQuery)

	return newBoolColumn
}

func (i *boolColumnImpl) From(subQuery SelectTable) ColumnBool {
	newBoolColumn := i.from(subQuery).(ColumnBool)

	return newBoolColumn
}

// BoolColumn creates named bool column.
func BoolColumn(name string) ColumnBool {
	boolColumn := &boolColumnImpl{}
	boolColumn.columnImpl = newColumn(name, "", boolColumn)
	boolColumn.boolInterfaceImpl.parent = boolColumn

	return boolColumn
}

//------------------------------------------------------//

// ColumnFloat is interface for SQL real, numeric, decimal or double precision column.
type ColumnFloat interface {
	FloatExpression
	column

	From(subQuery SelectTable) ColumnFloat
}

type floatColumnImpl struct {
	floatInterfaceImpl
	columnImpl
}

func (i *floatColumnImpl) from(subQuery SelectTable) projection {
	newFloatColumn := FloatColumn(i.name)
	newFloatColumn.setTableName(i.tableName)
	newFloatColumn.setSubQuery(subQuery)

	return newFloatColumn
}

func (i *floatColumnImpl) From(subQuery SelectTable) ColumnFloat {
	newFloatColumn := i.from(subQuery).(ColumnFloat)

	return newFloatColumn
}

// FloatColumn creates named float column.
func FloatColumn(name string) ColumnFloat {
	floatColumn := &floatColumnImpl{}
	floatColumn.floatInterfaceImpl.parent = floatColumn
	floatColumn.columnImpl = newColumn(name, "", floatColumn)

	return floatColumn
}

//------------------------------------------------------//

// ColumnInteger is interface for SQL smallint, integer, bigint columns.
type ColumnInteger interface {
	IntegerExpression
	column

	From(subQuery SelectTable) ColumnInteger
}

type integerColumnImpl struct {
	integerInterfaceImpl

	columnImpl
}

func (i *integerColumnImpl) from(subQuery SelectTable) projection {
	newIntColumn := IntegerColumn(i.name)
	newIntColumn.setTableName(i.tableName)
	newIntColumn.setSubQuery(subQuery)

	return newIntColumn
}

func (i *integerColumnImpl) From(subQuery SelectTable) ColumnInteger {
	return i.from(subQuery).(ColumnInteger)
}

// IntegerColumn creates named integer column.
func IntegerColumn(name string) ColumnInteger {
	integerColumn := &integerColumnImpl{}
	integerColumn.integerInterfaceImpl.parent = integerColumn
	integerColumn.columnImpl = newColumn(name, "", integerColumn)

	return integerColumn
}

//------------------------------------------------------//

// ColumnString is interface for SQL text, character, character varying
// bytea, uuid columns and enums types.
type ColumnString interface {
	StringExpression
	column

	From(subQuery SelectTable) ColumnString
}

type stringColumnImpl struct {
	stringInterfaceImpl

	columnImpl
}

func (i *stringColumnImpl) from(subQuery SelectTable) projection {
	newStrColumn := StringColumn(i.name)
	newStrColumn.setTableName(i.tableName)
	newStrColumn.setSubQuery(subQuery)

	return newStrColumn
}

func (i *stringColumnImpl) From(subQuery SelectTable) ColumnString {
	return i.from(subQuery).(ColumnString)
}

// StringColumn creates named string column.
func StringColumn(name string) ColumnString {
	stringColumn := &stringColumnImpl{}
	stringColumn.stringInterfaceImpl.parent = stringColumn
	stringColumn.columnImpl = newColumn(name, "", stringColumn)

	return stringColumn
}

//------------------------------------------------------//

// ColumnTime is interface for SQL time column.
type ColumnTime interface {
	TimeExpression
	column

	From(subQuery SelectTable) ColumnTime
}

type timeColumnImpl struct {
	timeInterfaceImpl
	columnImpl
}

func (i *timeColumnImpl) from(subQuery SelectTable) projection {
	newTimeColumn := TimeColumn(i.name)
	newTimeColumn.setTableName(i.tableName)
	newTimeColumn.setSubQuery(subQuery)

	return newTimeColumn
}

func (i *timeColumnImpl) From(subQuery SelectTable) ColumnTime {
	return i.from(subQuery).(ColumnTime)
}

// TimeColumn creates named time column
func TimeColumn(name string) ColumnTime {
	timeColumn := &timeColumnImpl{}
	timeColumn.timeInterfaceImpl.parent = timeColumn
	timeColumn.columnImpl = newColumn(name, "", timeColumn)
	return timeColumn
}

//------------------------------------------------------//

// ColumnTimez is interface of SQL time with time zone columns.
type ColumnTimez interface {
	TimezExpression
	column

	From(subQuery SelectTable) ColumnTimez
}

type timezColumnImpl struct {
	timezInterfaceImpl

	columnImpl
}

func (i *timezColumnImpl) from(subQuery SelectTable) projection {
	newTimezColumn := TimezColumn(i.name)
	newTimezColumn.setTableName(i.tableName)
	newTimezColumn.setSubQuery(subQuery)

	return newTimezColumn
}

func (i *timezColumnImpl) From(subQuery SelectTable) ColumnTimez {
	return i.from(subQuery).(ColumnTimez)
}

// TimezColumn creates named time with time zone column.
func TimezColumn(name string) ColumnTimez {
	timezColumn := &timezColumnImpl{}
	timezColumn.timezInterfaceImpl.parent = timezColumn
	timezColumn.columnImpl = newColumn(name, "", timezColumn)

	return timezColumn
}

//------------------------------------------------------//

// ColumnTimestamp is interface of SQL timestamp columns.
type ColumnTimestamp interface {
	TimestampExpression
	column

	From(subQuery SelectTable) ColumnTimestamp
}

type timestampColumnImpl struct {
	timestampInterfaceImpl

	columnImpl
}

func (i *timestampColumnImpl) from(subQuery SelectTable) projection {
	newTimestampColumn := TimestampColumn(i.name)
	newTimestampColumn.setTableName(i.tableName)
	newTimestampColumn.setSubQuery(subQuery)

	return newTimestampColumn
}

func (i *timestampColumnImpl) From(subQuery SelectTable) ColumnTimestamp {
	return i.from(subQuery).(ColumnTimestamp)
}

// TimestampColumn creates named timestamp column
func TimestampColumn(name string) ColumnTimestamp {
	timestampColumn := &timestampColumnImpl{}
	timestampColumn.timestampInterfaceImpl.parent = timestampColumn
	timestampColumn.columnImpl = newColumn(name, "", timestampColumn)

	return timestampColumn
}

//------------------------------------------------------//

// ColumnTimestampz is interface of SQL timestamp with timezone columns.
type ColumnTimestampz interface {
	TimestampzExpression
	column

	From(subQuery SelectTable) ColumnTimestampz
}

type timestampzColumnImpl struct {
	timestampzInterfaceImpl

	columnImpl
}

func (i *timestampzColumnImpl) from(subQuery SelectTable) projection {
	newTimestampzColumn := TimestampzColumn(i.name)
	newTimestampzColumn.setTableName(i.tableName)
	newTimestampzColumn.setSubQuery(subQuery)

	return newTimestampzColumn
}

func (i *timestampzColumnImpl) From(subQuery SelectTable) ColumnTimestampz {
	return i.from(subQuery).(ColumnTimestampz)
}

// TimestampzColumn creates named timestamp with time zone column.
func TimestampzColumn(name string) ColumnTimestampz {
	timestampzColumn := &timestampzColumnImpl{}
	timestampzColumn.timestampzInterfaceImpl.parent = timestampzColumn
	timestampzColumn.columnImpl = newColumn(name, "", timestampzColumn)

	return timestampzColumn
}

//------------------------------------------------------//

// ColumnDate is interface of SQL date columns.
type ColumnDate interface {
	DateExpression
	column

	From(subQuery SelectTable) ColumnDate
}

type dateColumnImpl struct {
	dateInterfaceImpl

	columnImpl
}

func (i *dateColumnImpl) from(subQuery SelectTable) projection {
	newDateColumn := DateColumn(i.name)
	newDateColumn.setTableName(i.tableName)
	newDateColumn.setSubQuery(subQuery)

	return newDateColumn
}

func (i *dateColumnImpl) From(subQuery SelectTable) ColumnDate {
	return i.from(subQuery).(ColumnDate)
}

// DateColumn creates named date column.
func DateColumn(name string) ColumnDate {
	dateColumn := &dateColumnImpl{}
	dateColumn.dateInterfaceImpl.parent = dateColumn
	dateColumn.columnImpl = newColumn(name, "", dateColumn)
	return dateColumn
}
