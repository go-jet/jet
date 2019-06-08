package sqlbuilder

//------------------------------------------------------//
type ColumnBool interface {
	BoolExpression
	column

	From(table ExpressionTable) ColumnBool
}

type boolColumnImpl struct {
	boolInterfaceImpl

	columnImpl
}

func (i *boolColumnImpl) From(table ExpressionTable) ColumnBool {
	newBoolColumn := BoolColumn(i.defaultAlias())
	newBoolColumn.setTableName(table.Alias())
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

	From(table ExpressionTable) ColumnFloat
}

type floatColumnImpl struct {
	floatInterfaceImpl
	columnImpl
}

func (i *floatColumnImpl) From(table ExpressionTable) ColumnFloat {
	newFloatColumn := FloatColumn(i.defaultAlias())
	newFloatColumn.setTableName(table.Alias())
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

	From(table ExpressionTable) ColumnInteger
}

type integerColumnImpl struct {
	integerInterfaceImpl

	columnImpl
}

func (i *integerColumnImpl) From(table ExpressionTable) ColumnInteger {
	newIntColumn := IntegerColumn(i.defaultAlias())
	newIntColumn.setTableName(table.Alias())
	return newIntColumn
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

	From(table ExpressionTable) ColumnString
}

type stringColumnImpl struct {
	stringInterfaceImpl

	columnImpl
}

func (i *stringColumnImpl) From(table ExpressionTable) ColumnString {
	newStrColumn := StringColumn(i.defaultAlias())
	newStrColumn.setTableName(table.Alias())
	return newStrColumn
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

	From(table ExpressionTable) ColumnTime
}

type timeColumnImpl struct {
	timeInterfaceImpl

	columnImpl
}

func (i *timeColumnImpl) From(table ExpressionTable) ColumnTime {
	newTimeColumn := TimeColumn(i.defaultAlias())
	newTimeColumn.setTableName(table.Alias())
	return newTimeColumn
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

	From(table ExpressionTable) ColumnTimez
}

type timezColumnImpl struct {
	timezInterfaceImpl

	columnImpl
}

func (i *timezColumnImpl) From(table ExpressionTable) ColumnTimez {
	newTimezColumn := TimezColumn(i.defaultAlias())
	newTimezColumn.setTableName(table.Alias())
	return newTimezColumn
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

	From(table ExpressionTable) ColumnTimestamp
}

type timestampColumnImpl struct {
	timestampInterfaceImpl

	columnImpl
}

func (i *timestampColumnImpl) From(table ExpressionTable) ColumnTimestamp {
	newTimestampColumn := TimestampColumn(i.defaultAlias())
	newTimestampColumn.setTableName(table.Alias())
	return newTimestampColumn
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

	From(table ExpressionTable) ColumnTimestampz
}

type timestampzColumnImpl struct {
	timestampzInterfaceImpl

	columnImpl
}

func (i *timestampzColumnImpl) From(table ExpressionTable) ColumnTimestampz {
	newTimestampzColumn := TimestampzColumn(i.defaultAlias())
	newTimestampzColumn.setTableName(table.Alias())
	return newTimestampzColumn
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

	From(table ExpressionTable) ColumnDate
}

type dateColumnImpl struct {
	dateInterfaceImpl

	columnImpl
}

func (i *dateColumnImpl) From(table ExpressionTable) ColumnDate {
	newDateColumn := DateColumn(i.defaultAlias())
	newDateColumn.setTableName(table.Alias())
	return newDateColumn
}

func DateColumn(name string) ColumnDate {
	dateColumn := &dateColumnImpl{}

	dateColumn.dateInterfaceImpl.parent = dateColumn

	dateColumn.columnImpl = newColumn(name, "", dateColumn)

	return dateColumn
}
