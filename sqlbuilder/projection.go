package sqlbuilder

type projection interface {
	serializeForProjection(statement statementType, out *queryData) error
}

//------------------------------------------------------//
// Dummy type for select * AllColumns
type ColumnList []column

func (cl ColumnList) isProjectionType() {}

func (cl ColumnList) serializeForProjection(statement statementType, out *queryData) error {
	for i, column := range cl {
		err := column.serializeForProjection(statement, out)

		if err != nil {
			return err
		}

		if i != len(cl)-1 {
			out.writeString(", ")
		}
	}
	return nil
}

func (cl ColumnList) DefaultAlias() []projection {
	newColumnList := []projection{}

	for _, column := range cl {
		newColumn := column.DefaultAlias()
		newColumnList = append(newColumnList, newColumn)
	}

	return newColumnList
}
