package sqlbuilder

import "bytes"

type Projection interface {
	SerializeForProjection(out *bytes.Buffer) error
}

//------------------------------------------------------//
// Dummy type for select * AllColumns
type ColumnList []Column

func (cl ColumnList) SerializeForProjection(out *bytes.Buffer) error {
	for i, column := range cl {
		err := column.SerializeSql(out, FOR_PROJECTION)

		if err != nil {
			return err
		}

		if i != len(cl)-1 {
			out.WriteString(", ")
		}
	}
	return nil
}
