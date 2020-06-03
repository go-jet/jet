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
	ColumnExpressionImpl
}

func (i *boolColumnImpl) From(subQuery SelectTable) ColumnBool {
	newBoolColumn := BoolColumn(i.name)
	newBoolColumn.setTableName(i.tableName)
	newBoolColumn.setSubQuery(subQuery)

	return newBoolColumn
}

func (i *boolColumnImpl) SET(boolExp BoolExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:     i,
		expression: boolExp,
	}
}

// BoolColumn creates named bool column.
func BoolColumn(name string) ColumnBool {
	boolColumn := &boolColumnImpl{}
	boolColumn.ColumnExpressionImpl = NewColumnImpl(name, "", boolColumn)
	boolColumn.boolInterfaceImpl.parent = boolColumn

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
	ColumnExpressionImpl
}

func (i *floatColumnImpl) From(subQuery SelectTable) ColumnFloat {
	newFloatColumn := FloatColumn(i.name)
	newFloatColumn.setTableName(i.tableName)
	newFloatColumn.setSubQuery(subQuery)

	return newFloatColumn
}

func (i *floatColumnImpl) SET(floatExp FloatExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:     i,
		expression: floatExp,
	}
}

// FloatColumn creates named float column.
func FloatColumn(name string) ColumnFloat {
	floatColumn := &floatColumnImpl{}
	floatColumn.floatInterfaceImpl.parent = floatColumn
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

	ColumnExpressionImpl
}

func (i *integerColumnImpl) From(subQuery SelectTable) ColumnInteger {
	newIntColumn := IntegerColumn(i.name)
	newIntColumn.setTableName(i.tableName)
	newIntColumn.setSubQuery(subQuery)

	return newIntColumn
}

func (i *integerColumnImpl) SET(intExp IntegerExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:     i,
		expression: intExp,
	}
}

// IntegerColumn creates named integer column.
func IntegerColumn(name string) ColumnInteger {
	integerColumn := &integerColumnImpl{}
	integerColumn.integerInterfaceImpl.parent = integerColumn
	integerColumn.ColumnExpressionImpl = NewColumnImpl(name, "", integerColumn)

	return integerColumn
}

//------------------------------------------------------//

// ColumnString is interface for SQL text, character, character varying
// bytea, uuid columns and enums types.
type ColumnString interface {
	StringExpression
	Column

	From(subQuery SelectTable) ColumnString
	SET(stringExp StringExpression) ColumnAssigment
}

type stringColumnImpl struct {
	stringInterfaceImpl

	ColumnExpressionImpl
}

func (i *stringColumnImpl) From(subQuery SelectTable) ColumnString {
	newStrColumn := StringColumn(i.name)
	newStrColumn.setTableName(i.tableName)
	newStrColumn.setSubQuery(subQuery)

	return newStrColumn
}

func (i *stringColumnImpl) SET(stringExp StringExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:     i,
		expression: stringExp,
	}
}

// StringColumn creates named string column.
func StringColumn(name string) ColumnString {
	stringColumn := &stringColumnImpl{}
	stringColumn.stringInterfaceImpl.parent = stringColumn
	stringColumn.ColumnExpressionImpl = NewColumnImpl(name, "", stringColumn)

	return stringColumn
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
	ColumnExpressionImpl
}

func (i *timeColumnImpl) From(subQuery SelectTable) ColumnTime {
	newTimeColumn := TimeColumn(i.name)
	newTimeColumn.setTableName(i.tableName)
	newTimeColumn.setSubQuery(subQuery)

	return newTimeColumn
}

func (i *timeColumnImpl) SET(timeExp TimeExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:     i,
		expression: timeExp,
	}
}

// TimeColumn creates named time column
func TimeColumn(name string) ColumnTime {
	timeColumn := &timeColumnImpl{}
	timeColumn.timeInterfaceImpl.parent = timeColumn
	timeColumn.ColumnExpressionImpl = NewColumnImpl(name, "", timeColumn)
	return timeColumn
}

//------------------------------------------------------//

// ColumnTimez is interface of SQL time with time zone columns.
type ColumnTimez interface {
	TimezExpression
	Column

	From(subQuery SelectTable) ColumnTimez
}

type timezColumnImpl struct {
	timezInterfaceImpl
	ColumnExpressionImpl
}

func (i *timezColumnImpl) From(subQuery SelectTable) ColumnTimez {
	newTimezColumn := TimezColumn(i.name)
	newTimezColumn.setTableName(i.tableName)
	newTimezColumn.setSubQuery(subQuery)

	return newTimezColumn
}

func (i *timezColumnImpl) SET(timezExp TimezExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:     i,
		expression: timezExp,
	}
}

// TimezColumn creates named time with time zone column.
func TimezColumn(name string) ColumnTimez {
	timezColumn := &timezColumnImpl{}
	timezColumn.timezInterfaceImpl.parent = timezColumn
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
	ColumnExpressionImpl
}

func (i *timestampColumnImpl) From(subQuery SelectTable) ColumnTimestamp {
	newTimestampColumn := TimestampColumn(i.name)
	newTimestampColumn.setTableName(i.tableName)
	newTimestampColumn.setSubQuery(subQuery)

	return newTimestampColumn
}

func (i *timestampColumnImpl) SET(timestampExp TimestampExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:     i,
		expression: timestampExp,
	}
}

// TimestampColumn creates named timestamp column
func TimestampColumn(name string) ColumnTimestamp {
	timestampColumn := &timestampColumnImpl{}
	timestampColumn.timestampInterfaceImpl.parent = timestampColumn
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
	ColumnExpressionImpl
}

func (i *timestampzColumnImpl) From(subQuery SelectTable) ColumnTimestampz {
	newTimestampzColumn := TimestampzColumn(i.name)
	newTimestampzColumn.setTableName(i.tableName)
	newTimestampzColumn.setSubQuery(subQuery)

	return newTimestampzColumn
}

func (i *timestampzColumnImpl) SET(timestampzExp TimestampzExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:     i,
		expression: timestampzExp,
	}
}

// TimestampzColumn creates named timestamp with time zone column.
func TimestampzColumn(name string) ColumnTimestampz {
	timestampzColumn := &timestampzColumnImpl{}
	timestampzColumn.timestampzInterfaceImpl.parent = timestampzColumn
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
	ColumnExpressionImpl
}

func (i *dateColumnImpl) From(subQuery SelectTable) ColumnDate {
	newDateColumn := DateColumn(i.name)
	newDateColumn.setTableName(i.tableName)
	newDateColumn.setSubQuery(subQuery)

	return newDateColumn
}

func (i *dateColumnImpl) SET(dateExp DateExpression) ColumnAssigment {
	return columnAssigmentImpl{
		column:     i,
		expression: dateExp,
	}
}

// DateColumn creates named date column.
func DateColumn(name string) ColumnDate {
	dateColumn := &dateColumnImpl{}
	dateColumn.dateInterfaceImpl.parent = dateColumn
	dateColumn.ColumnExpressionImpl = NewColumnImpl(name, "", dateColumn)
	return dateColumn
}
