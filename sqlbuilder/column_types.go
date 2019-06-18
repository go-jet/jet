package sqlbuilder

//------------------------------------------------------//
type ColumnBool interface {
	BoolExpression
	column

	From(subQuery ExpressionTable) ColumnBool
}

type boolColumnImpl struct {
	boolInterfaceImpl

	columnImpl
}

func (i *boolColumnImpl) from(subQuery ExpressionTable) projection {
	newBoolColumn := BoolColumn(i.name)
	newBoolColumn.setTableName(i.tableName)
	newBoolColumn.setSubQuery(subQuery)

	return newBoolColumn
}

func (i *boolColumnImpl) From(subQuery ExpressionTable) ColumnBool {
	newBoolColumn := i.from(subQuery).(ColumnBool)

	return newBoolColumn
}

func BoolColumn(name string) ColumnBool {
	boolColumn := &boolColumnImpl{}
	boolColumn.columnImpl = newColumn(name, "", boolColumn)
	boolColumn.boolInterfaceImpl.parent = boolColumn

	return boolColumn
}

//------------------------------------------------------//
type ColumnFloat interface {
	FloatExpression
	column

	From(subQuery ExpressionTable) ColumnFloat
}

type floatColumnImpl struct {
	floatInterfaceImpl
	columnImpl
}

func (i *floatColumnImpl) from(subQuery ExpressionTable) projection {
	newFloatColumn := FloatColumn(i.name)
	newFloatColumn.setTableName(i.tableName)
	newFloatColumn.setSubQuery(subQuery)

	return newFloatColumn
}

func (i *floatColumnImpl) From(subQuery ExpressionTable) ColumnFloat {
	newFloatColumn := i.from(subQuery).(ColumnFloat)

	return newFloatColumn
}

func FloatColumn(name string) ColumnFloat {
	floatColumn := &floatColumnImpl{}
	floatColumn.floatInterfaceImpl.parent = floatColumn
	floatColumn.columnImpl = newColumn(name, "", floatColumn)

	return floatColumn
}

//------------------------------------------------------//
type ColumnInteger interface {
	IntegerExpression
	column

	From(subQuery ExpressionTable) ColumnInteger
}

type integerColumnImpl struct {
	integerInterfaceImpl

	columnImpl
}

func (i *integerColumnImpl) from(subQuery ExpressionTable) projection {
	newIntColumn := IntegerColumn(i.name)
	newIntColumn.setTableName(i.tableName)
	newIntColumn.setSubQuery(subQuery)

	return newIntColumn
}

func (i *integerColumnImpl) From(subQuery ExpressionTable) ColumnInteger {
	return i.from(subQuery).(ColumnInteger)
}

func IntegerColumn(name string) ColumnInteger {
	integerColumn := &integerColumnImpl{}
	integerColumn.integerInterfaceImpl.parent = integerColumn
	integerColumn.columnImpl = newColumn(name, "", integerColumn)

	return integerColumn
}

//------------------------------------------------------//
type ColumnString interface {
	StringExpression
	column

	From(subQuery ExpressionTable) ColumnString
}

type stringColumnImpl struct {
	stringInterfaceImpl

	columnImpl
}

func (i *stringColumnImpl) from(subQuery ExpressionTable) projection {
	newStrColumn := StringColumn(i.name)
	newStrColumn.setTableName(i.tableName)
	newStrColumn.setSubQuery(subQuery)

	return newStrColumn
}

func (i *stringColumnImpl) From(subQuery ExpressionTable) ColumnString {
	return i.from(subQuery).(ColumnString)
}

func StringColumn(name string) ColumnString {
	stringColumn := &stringColumnImpl{}
	stringColumn.stringInterfaceImpl.parent = stringColumn
	stringColumn.columnImpl = newColumn(name, "", stringColumn)

	return stringColumn
}

//------------------------------------------------------//
type ColumnTime interface {
	TimeExpression
	column

	From(subQuery ExpressionTable) ColumnTime
}

type timeColumnImpl struct {
	timeInterfaceImpl
	columnImpl
}

func (i *timeColumnImpl) from(subQuery ExpressionTable) projection {
	newTimeColumn := TimeColumn(i.name)
	newTimeColumn.setTableName(i.tableName)
	newTimeColumn.setSubQuery(subQuery)

	return newTimeColumn
}

func (i *timeColumnImpl) From(subQuery ExpressionTable) ColumnTime {
	return i.from(subQuery).(ColumnTime)
}

func TimeColumn(name string) ColumnTime {
	timeColumn := &timeColumnImpl{}
	timeColumn.timeInterfaceImpl.parent = timeColumn
	timeColumn.columnImpl = newColumn(name, "", timeColumn)
	return timeColumn
}

//------------------------------------------------------//

type ColumnTimez interface {
	TimezExpression
	column

	From(subQuery ExpressionTable) ColumnTimez
}

type timezColumnImpl struct {
	timezInterfaceImpl

	columnImpl
}

func (i *timezColumnImpl) from(subQuery ExpressionTable) projection {
	newTimezColumn := TimezColumn(i.name)
	newTimezColumn.setTableName(i.tableName)
	newTimezColumn.setSubQuery(subQuery)

	return newTimezColumn
}

func (i *timezColumnImpl) From(subQuery ExpressionTable) ColumnTimez {
	return i.from(subQuery).(ColumnTimez)
}

func TimezColumn(name string) ColumnTimez {
	timezColumn := &timezColumnImpl{}
	timezColumn.timezInterfaceImpl.parent = timezColumn
	timezColumn.columnImpl = newColumn(name, "", timezColumn)

	return timezColumn
}

//------------------------------------------------------//
type ColumnTimestamp interface {
	TimestampExpression
	column

	From(subQuery ExpressionTable) ColumnTimestamp
}

type timestampColumnImpl struct {
	timestampInterfaceImpl

	columnImpl
}

func (i *timestampColumnImpl) from(subQuery ExpressionTable) projection {
	newTimestampColumn := TimestampColumn(i.name)
	newTimestampColumn.setTableName(i.tableName)
	newTimestampColumn.setSubQuery(subQuery)

	return newTimestampColumn
}

func (i *timestampColumnImpl) From(subQuery ExpressionTable) ColumnTimestamp {
	return i.from(subQuery).(ColumnTimestamp)
}

func TimestampColumn(name string) ColumnTimestamp {
	timestampColumn := &timestampColumnImpl{}
	timestampColumn.timestampInterfaceImpl.parent = timestampColumn
	timestampColumn.columnImpl = newColumn(name, "", timestampColumn)

	return timestampColumn
}

//------------------------------------------------------//
type ColumnTimestampz interface {
	TimestampzExpression
	column

	From(subQuery ExpressionTable) ColumnTimestampz
}

type timestampzColumnImpl struct {
	timestampzInterfaceImpl

	columnImpl
}

func (i *timestampzColumnImpl) from(subQuery ExpressionTable) projection {
	newTimestampzColumn := TimestampzColumn(i.name)
	newTimestampzColumn.setTableName(i.tableName)
	newTimestampzColumn.setSubQuery(subQuery)

	return newTimestampzColumn
}

func (i *timestampzColumnImpl) From(subQuery ExpressionTable) ColumnTimestampz {
	return i.from(subQuery).(ColumnTimestampz)
}

func TimestampzColumn(name string) ColumnTimestampz {
	timestampzColumn := &timestampzColumnImpl{}
	timestampzColumn.timestampzInterfaceImpl.parent = timestampzColumn
	timestampzColumn.columnImpl = newColumn(name, "", timestampzColumn)

	return timestampzColumn
}

//------------------------------------------------------//
type ColumnDate interface {
	DateExpression
	column

	From(subQuery ExpressionTable) ColumnDate
}

type dateColumnImpl struct {
	dateInterfaceImpl

	columnImpl
}

func (i *dateColumnImpl) from(subQuery ExpressionTable) projection {
	newDateColumn := DateColumn(i.name)
	newDateColumn.setTableName(i.tableName)
	newDateColumn.setSubQuery(subQuery)

	return newDateColumn
}

func (i *dateColumnImpl) From(subQuery ExpressionTable) ColumnDate {
	return i.from(subQuery).(ColumnDate)
}

func DateColumn(name string) ColumnDate {
	dateColumn := &dateColumnImpl{}
	dateColumn.dateInterfaceImpl.parent = dateColumn
	dateColumn.columnImpl = newColumn(name, "", dateColumn)
	return dateColumn
}
