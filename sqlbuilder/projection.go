package sqlbuilder

type Projection interface {
	SerializeForProjection(out *queryData) error
}

//------------------------------------------------------//
// Dummy type for select * AllColumns
type ColumnList []Column

func (cl ColumnList) SerializeForProjection(out *queryData) error {
	for i, column := range cl {
		err := column.Serialize(out, FOR_PROJECTION)

		if err != nil {
			return err
		}

		if i != len(cl)-1 {
			out.WriteString(", ")
		}
	}
	return nil
}
