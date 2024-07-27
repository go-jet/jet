package jet

// ColumnList is a helper type to support list of columns as single projection
type ColumnList []ColumnExpression

// SET creates column assigment for each column in column list. expression should be created by ROW function
//
//	Link.UPDATE().
//		SET(Link.MutableColumns.SET(ROW(String("github.com"), Bool(false))).
//		WHERE(Link.ID.EQ(Int(0)))
func (cl ColumnList) SET(expression Expression) ColumnAssigment {
	return columnAssigmentImpl{
		column:     cl,
		expression: expression,
	}
}

// Except will create new column list in which columns contained in list of excluded column names are removed
//
//	Address.AllColumns.Except(Address.PostalCode, Address.Phone)
func (cl ColumnList) Except(excludedColumns ...Column) ColumnList {
	excludedColumnList := UnwidColumnList(excludedColumns)
	excludedColumnNames := map[string]bool{}

	for _, excludedColumn := range excludedColumnList {
		excludedColumnNames[excludedColumn.Name()] = true
	}

	var ret ColumnList

	for _, column := range cl {
		if excludedColumnNames[column.Name()] {
			continue
		}

		ret = append(ret, column)
	}

	return ret
}

// As will create new projection list where each column is wrapped with a new table alias.
// tableAlias should be in the form 'name' or 'name.*', or it can also be an empty string.
// For instance: If projection list has a column 'Artist.Name', and tableAlias is 'Musician.*', returned projection list will
// have a column wrapped in alias 'Musician.Name'. If tableAlias is empty string, it removes existing table alias ('Artist.Name' becomes 'Name').
func (cl ColumnList) As(tableAlias string) ProjectionList {
	ret := make(ProjectionList, 0, len(cl))
	for _, c := range cl {
		ret = append(ret, c.AS(joinAlias(tableAlias, c.Name())))
	}
	return ret
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
