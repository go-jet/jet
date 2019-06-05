package sqlbuilder

//------------------------------------------------------//
type BoolColumn struct {
	boolInterfaceImpl

	baseColumn
}

func NewBoolColumn(name string, isNullable bool) *BoolColumn {

	boolColumn := &BoolColumn{}
	boolColumn.baseColumn = newBaseColumn(name, isNullable, "", boolColumn)

	boolColumn.boolInterfaceImpl.parent = boolColumn

	return boolColumn
}

//------------------------------------------------------//
type FloatColumn struct {
	floatInterfaceImpl
	baseColumn
}

func NewFloatColumn(name string, isNullable bool) *FloatColumn {

	floatColumn := &FloatColumn{}

	floatColumn.floatInterfaceImpl.parent = floatColumn

	floatColumn.baseColumn = newBaseColumn(name, isNullable, "", floatColumn)

	return floatColumn
}

//------------------------------------------------------//
type IntegerColumn struct {
	integerInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewIntegerColumn(name string, isNullable bool) *IntegerColumn {
	integerColumn := &IntegerColumn{}

	integerColumn.integerInterfaceImpl.parent = integerColumn

	integerColumn.baseColumn = newBaseColumn(name, isNullable, "", integerColumn)

	return integerColumn
}

//------------------------------------------------------//
type StringColumn struct {
	stringInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewStringColumn(name string, isNullable bool) *StringColumn {

	stringColumn := &StringColumn{}

	stringColumn.stringInterfaceImpl.parent = stringColumn

	stringColumn.baseColumn = newBaseColumn(name, isNullable, "", stringColumn)

	return stringColumn
}

//------------------------------------------------------//
type TimeColumn struct {
	timeInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewTimeColumn(name string, isNullable bool) *TimeColumn {
	timeColumn := &TimeColumn{}

	timeColumn.timeInterfaceImpl.parent = timeColumn

	timeColumn.baseColumn = newBaseColumn(name, isNullable, "", timeColumn)

	return timeColumn
}

//------------------------------------------------------//
type TimezColumn struct {
	timezInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewTimezColumn(name string, isNullable bool) *TimezColumn {
	timezColumn := &TimezColumn{}

	timezColumn.timezInterfaceImpl.parent = timezColumn

	timezColumn.baseColumn = newBaseColumn(name, isNullable, "", timezColumn)

	return timezColumn
}

//------------------------------------------------------//
type TimestampColumn struct {
	timestampInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewTimestampColumn(name string, isNullable bool) *TimestampColumn {
	timestampColumn := &TimestampColumn{}

	timestampColumn.timestampInterfaceImpl.parent = timestampColumn

	timestampColumn.baseColumn = newBaseColumn(name, isNullable, "", timestampColumn)

	return timestampColumn
}

//------------------------------------------------------//
type TimestampzColumn struct {
	timestampzInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewTimestampzColumn(name string, isNullable bool) *TimestampzColumn {
	timestampzColumn := &TimestampzColumn{}

	timestampzColumn.timestampzInterfaceImpl.parent = timestampzColumn

	timestampzColumn.baseColumn = newBaseColumn(name, isNullable, "", timestampzColumn)

	return timestampzColumn
}

//------------------------------------------------------//
type DateColumn struct {
	dateInterfaceImpl

	baseColumn
}

// Representation of any integer column
// This function will panic if name is not valid
func NewDateColumn(name string, isNullable bool) *DateColumn {
	dateColumn := &DateColumn{}

	dateColumn.dateInterfaceImpl.parent = dateColumn

	dateColumn.baseColumn = newBaseColumn(name, isNullable, "", dateColumn)

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
