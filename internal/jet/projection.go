package jet

// Projection is interface for all projection types. Types that can be part of, for instance SELECT clause.
type Projection interface {
	serializeForProjection(statement StatementType, out *SQLBuilder)
	fromImpl(subQuery SelectTable) Projection
}

// SerializeForProjection is helper function for serializing projection outside of jet package
func SerializeForProjection(projection Projection, statementType StatementType, out *SQLBuilder) {
	projection.serializeForProjection(statementType, out)
}

// ProjectionList is a redefined type, so that ProjectionList can be used as a Projection.
type ProjectionList []Projection

func (pl ProjectionList) fromImpl(subQuery SelectTable) Projection {
	newProjectionList := ProjectionList{}

	for _, projection := range pl {
		newProjectionList = append(newProjectionList, projection.fromImpl(subQuery))
	}

	return newProjectionList
}

func (pl ProjectionList) serializeForProjection(statement StatementType, out *SQLBuilder) {
	SerializeProjectionList(statement, pl, out)
}

// As will create new projection list where each column is wrapped with a new table alias.
// tableAlias should be in the form 'name' or 'name.*', or it can be an empty string, which will remove existing table alias.
// For instance: If projection list has a column 'Artist.Name', and tableAlias is 'Musician.*', returned projection list will
// have a column wrapped in alias 'Musician.Name'. If tableAlias is empty string, it removes existing table alias ('Artist.Name' becomes 'Name').
func (pl ProjectionList) As(tableAlias string) ProjectionList {
	newProjectionList := ProjectionList{}

	for _, projection := range pl {
		switch p := projection.(type) {
		case ProjectionList:
			newProjectionList = append(newProjectionList, p.As(tableAlias))
		case ColumnList:
			newProjectionList = append(newProjectionList, p.As(tableAlias))
		case ColumnExpression:
			newProjectionList = append(newProjectionList, newAlias(p, joinAlias(tableAlias, p.Name())))
		case *alias:
			newAlias := *p
			_, columnName := extractTableAndColumnName(newAlias.alias)
			newAlias.alias = joinAlias(tableAlias, columnName)
			newProjectionList = append(newProjectionList, &newAlias)
		}
	}

	return newProjectionList
}

// Except will create new projection list in which columns contained in excluded column names are removed
func (pl ProjectionList) Except(toExclude ...Column) ProjectionList {
	excludedColumnList := UnwidColumnList(toExclude)
	excludedColumnNames := map[string]bool{}

	for _, excludedColumn := range excludedColumnList {
		excludedColumnNames[excludedColumn.Name()] = true
	}

	var ret ProjectionList

	for _, projection := range pl {
		switch p := projection.(type) {
		case ProjectionList:
			ret = append(ret, p.Except(toExclude...))
		case ColumnExpression:
			if excludedColumnNames[p.Name()] {
				continue
			}
			ret = append(ret, p)
		}
	}

	return ret
}
