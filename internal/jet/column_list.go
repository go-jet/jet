package jet

// ColumnList is a helper type to support list of columns as single projection
type ColumnList []ColumnExpression

// SET creates column assigment for each column in column list. expression should be created by ROW function
func (cl ColumnList) SET(expression Expression) ColumnAssigment {
	return columnAssigmentImpl{
		column:     cl,
		expression: expression,
	}
}

func (cl ColumnList) fromImpl(subQuery SelectTable) Projection {
	newProjectionList := ProjectionList{}

	for _, column := range cl {
		newProjectionList = append(newProjectionList, column.fromImpl(subQuery))
	}

	return newProjectionList
}

func (cl ColumnList) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("(")
	for i, column := range cl {
		if i > 0 {
			out.WriteString(", ")
		}
		column.serialize(statement, out, FallTrough(options)...)
	}
	out.WriteString(")")
}

func (cl ColumnList) serializeForProjection(statement StatementType, out *SQLBuilder) {
	projections := ColumnListToProjectionList(cl)

	SerializeProjectionList(statement, projections, out)
}

// dummy column interface implementation

// Name is placeholder for ColumnList to implement Column interface
func (cl ColumnList) Name() string { return "" }

// TableName is placeholder for ColumnList to implement Column interface
func (cl ColumnList) TableName() string                { return "" }
func (cl ColumnList) setTableName(name string)         {}
func (cl ColumnList) setSubQuery(subQuery SelectTable) {}
func (cl ColumnList) defaultAlias() string             { return "" }

// SetTableName is utility function to set table name from outside of jet package to avoid making public setTableName
func SetTableName(columnExpression ColumnExpression, tableName string) {
	columnExpression.setTableName(tableName)
}

// SetSubQuery is utility function to set table name from outside of jet package to avoid making public setSubQuery
func SetSubQuery(columnExpression ColumnExpression, subQuery SelectTable) {
	columnExpression.setSubQuery(subQuery)
}
