package sqlbuilder

//------------------------------------------------------//
type BoolColumn struct {
	boolInterfaceImpl

	baseColumn
}

func NewBoolColumn(name string, nullable NullableColumn) *BoolColumn {

	boolColumn := &BoolColumn{}
	boolColumn.baseColumn = newBaseColumn(name, nullable, "", boolColumn)

	boolColumn.boolInterfaceImpl.parent = boolColumn

	return boolColumn
}

//------------------------------------------------------//
type FloatColumn struct {
	floatInterfaceImpl
	baseColumn
}

func NewFloatColumn(name string, nullable NullableColumn) *FloatColumn {

	floatColumn := &FloatColumn{}

	floatColumn.floatInterfaceImpl.parent = floatColumn

	floatColumn.baseColumn = newBaseColumn(name, nullable, "", floatColumn)

	return floatColumn
}

//------------------------------------------------------//
type IntegerColumn struct {
	integerInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewIntegerColumn(name string, nullable NullableColumn) *IntegerColumn {
	integerColumn := &IntegerColumn{}

	integerColumn.integerInterfaceImpl.parent = integerColumn

	integerColumn.baseColumn = newBaseColumn(name, nullable, "", integerColumn)

	return integerColumn
}

//------------------------------------------------------//
type StringColumn struct {
	stringInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewStringColumn(name string, nullable NullableColumn) *StringColumn {

	stringColumn := &StringColumn{}

	stringColumn.stringInterfaceImpl.parent = stringColumn

	stringColumn.baseColumn = newBaseColumn(name, nullable, "", stringColumn)

	return stringColumn
}

//------------------------------------------------------//
type TimeColumn struct {
	timeInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewTimeColumn(name string, nullable NullableColumn) *TimeColumn {
	timeColumn := &TimeColumn{}

	timeColumn.timeInterfaceImpl.parent = timeColumn

	timeColumn.baseColumn = newBaseColumn(name, nullable, "", timeColumn)

	return timeColumn
}

//------------------------------------------------------//
type TimezColumn struct {
	timezInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewTimezColumn(name string, nullable NullableColumn) *TimezColumn {
	timezColumn := &TimezColumn{}

	timezColumn.timezInterfaceImpl.parent = timezColumn

	timezColumn.baseColumn = newBaseColumn(name, nullable, "", timezColumn)

	return timezColumn
}

//------------------------------------------------------//
type TimestampColumn struct {
	timestampInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewTimestampColumn(name string, nullable NullableColumn) *TimestampColumn {
	timestampColumn := &TimestampColumn{}

	timestampColumn.timestampInterfaceImpl.parent = timestampColumn

	timestampColumn.baseColumn = newBaseColumn(name, nullable, "", timestampColumn)

	return timestampColumn
}

//------------------------------------------------------//
type TimestampzColumn struct {
	timestampzInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewTimestampzColumn(name string, nullable NullableColumn) *TimestampzColumn {
	timestampzColumn := &TimestampzColumn{}

	timestampzColumn.timestampzInterfaceImpl.parent = timestampzColumn

	timestampzColumn.baseColumn = newBaseColumn(name, nullable, "", timestampzColumn)

	return timestampzColumn
}

//------------------------------------------------------//
type DateColumn struct {
	dateInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewDateColumn(name string, nullable NullableColumn) *DateColumn {
	dateColumn := &DateColumn{}

	dateColumn.dateInterfaceImpl.parent = dateColumn

	dateColumn.baseColumn = newBaseColumn(name, nullable, "", dateColumn)

	return dateColumn
}

// ------------------------------------------------------//
type refColumn struct {
	baseColumn
}

func RefColumn(name string) *refColumn {
	refColumn := &refColumn{}
	refColumn.baseColumn = newBaseColumn(name, false, "", refColumn)

	return refColumn
}
