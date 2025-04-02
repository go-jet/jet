package jet

import "fmt"

// ColumnList is a helper type to support list of columns as single projection
type ColumnList []ColumnExpression

func (cl ColumnList) isExpressionOrColumnList() {}

// SET creates a column assignment from the current ColumnList using the provided expression.
// This assignment can be used in INSERT queries (e.g., to set columns on conflict) or in UPDATE queries
// (e.g., to assign new values to columns).
//
// The expression can be:
//   - Another ColumnList: It must have the same length as the current ColumnList and each column must match by name
//   - A ROW expression containing values.
//   - A SELECT statement that returns a matching column list structure.
//
// Examples:
//
//	Link.AllColumns.SET(ROW(String("github.com"), Bool(false)))
//
//	Link.MutableColumns.SET(Link.EXCLUDED.MutableColumns)
//
//	Link.MutableColumns.SET(
//	  SELECT(Link.MutableColumns).
//	    FROM(Link).
//	    WHERE(Link.ID.EQ(Int(200))),
//	)
func (cl ColumnList) SET(toAssignExp expressionOrColumnList) ColumnAssigment {

	if toAssign, ok := toAssignExp.(ColumnList); ok {
		if len(cl) != len(toAssign) {
			panic(fmt.Sprintf("jet: column list length mismatch: expected %d columns, got %d", len(cl), len(toAssign)))
		}

		var ret columnListAssigment

		for i, column := range cl {
			if column.Name() != toAssign[i].Name() {
				panic(fmt.Sprintf("jet: column name mismatch at index %d: expected column '%s', got '%s'",
					i, column.Name(), toAssign[i].Name(),
				))
			}

			ret = append(ret, columnAssigmentImpl{
				column:   column,
				toAssign: toAssign[i],
			})
		}

		return ret
	}

	return columnAssigmentImpl{
		column:   cl,
		toAssign: toAssignExp,
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

// From creates a new ColumnList that references the specified subquery.
// This method is typically used to project columns from a subquery into the surrounding query.
func (cl ColumnList) From(subQuery SelectTable) ColumnList {
	var ret ColumnList

	for _, column := range cl {
		ret = append(ret, column.fromImpl(subQuery).(ColumnExpression))
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

func (cl ColumnList) serializeForJsonObjEntry(statement StatementType, out *SQLBuilder) {
	projections := ColumnListToProjectionList(cl)

	SerializeProjectionListJsonObj(statement, projections, out)
}

func (cl ColumnList) serializeForRowToJsonProjection(statement StatementType, out *SQLBuilder) {
	projections := ColumnListToProjectionList(cl)

	out.WriteRowToJsonProjections(statement, projections)
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
