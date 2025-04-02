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
// We need to know type to encode value for json unmarshal. At the moment only bool, time and blob columns are of interest,
// so we don't have to support every column type.
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
