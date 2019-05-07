package sqlbuilder

type projection interface {
	serializeForProjection(out *queryData) error
}

//------------------------------------------------------//
// Dummy type for select * AllColumns
type ColumnList []column

func (cl ColumnList) isProjectionType() {}

func (cl ColumnList) serializeForProjection(out *queryData) error {
	for i, column := range cl {
		err := column.serializeForProjection(out)

		if err != nil {
			return err
		}

		if i != len(cl)-1 {
			out.WriteString(", ")
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
