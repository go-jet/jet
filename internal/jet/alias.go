package jet

type alias struct {
	expression Expression
	alias      string
}

func newAlias(expression Expression, aliasName string) Projection {
	return &alias{
		expression: expression,
		alias:      aliasName,
	}
}

func (a *alias) fromImpl(subQuery SelectTable) Projection {
	// if alias is in the form "table.column", we break it into two parts so that ProjectionList.As(newAlias) can
	// overwrite tableName with a new alias. This method is called only for exporting aliased custom columns.
	// Generated columns have default aliasing.
	tableName, columnName := extractTableAndColumnName(a.alias)

	newDummyColumn := newDummyColumnForExpression(a.expression, columnName)
	newDummyColumn.setTableName(tableName)
	newDummyColumn.setSubQuery(subQuery)

	return newDummyColumn
}

// This function is used to create dummy columns when exporting sub-query columns using subQuery.AllColumns()
// In most case we don't care about type of the column, except when sub-query columns are used as SELECT_JSON projection.
// We need to know type to encode value for json unmarshal.
func newDummyColumnForExpression(exp Expression, name string) ColumnExpression {

	switch exp.(type) {
	case BoolExpression:
		return BoolColumn(name)
	case IntegerExpression:
		return IntegerColumn(name)
	case FloatExpression:
		return FloatColumn(name)
	case BlobExpression:
		return BlobColumn(name)
	case DateExpression:
		return DateColumn(name)
	case TimeExpression:
		return TimeColumn(name)
	case TimezExpression:
		return TimezColumn(name)
	case TimestampExpression:
		return TimestampColumn(name)
	case TimestampzExpression:
		return TimestampzColumn(name)
	case IntervalExpression:
		return IntervalColumn(name)
	case StringExpression:
		return StringColumn(name)

	case Array[BoolExpression]:
		return ArrayColumn[BoolExpression](name)
	case Array[IntegerExpression]:
		return ArrayColumn[IntegerExpression](name)
	case Array[FloatExpression]:
		return ArrayColumn[FloatExpression](name)
	case Array[BlobExpression]:
		return ArrayColumn[BlobExpression](name)
	case Array[DateExpression]:
		return ArrayColumn[DateExpression](name)
	case Array[TimeExpression]:
		return ArrayColumn[TimeExpression](name)
	case Array[TimezExpression]:
		return ArrayColumn[TimezExpression](name)
	case Array[TimestampExpression]:
		return ArrayColumn[TimestampExpression](name)
	case Array[TimestampzExpression]:
		return ArrayColumn[TimestampzExpression](name)
	case Array[IntervalExpression]:
		return ArrayColumn[IntervalExpression](name)
	case Array[StringExpression]:
		return ArrayColumn[StringExpression](name)

	case Range[Int4Expression], Range[Int8Expression]:
		return RangeColumn[IntegerExpression](name)
	case Range[NumericExpression]:
		return RangeColumn[NumericExpression](name)
	case Range[DateExpression]:
		return RangeColumn[DateExpression](name)
	case Range[TimestampExpression]:
		return RangeColumn[TimestampExpression](name)
	case Range[TimestampzExpression]:
		return RangeColumn[TimestampzExpression](name)

	}

	return StringColumn(name)
}

func (a *alias) serializeForProjection(statement StatementType, out *SQLBuilder) {
	a.expression.serialize(statement, out)

	out.WriteString("AS")
	out.WriteAlias(a.alias)
}

func (a *alias) serializeForJsonObjEntry(statement StatementType, out *SQLBuilder) {
	out.WriteJsonObjKey(a.alias)
	a.expression.serializeForJsonValue(statement, out)
}

func (a *alias) serializeForRowToJsonProjection(statement StatementType, out *SQLBuilder) {
	a.expression.serializeForJsonValue(statement, out)

	out.WriteString("AS")
	out.WriteAlias(a.alias)
}
