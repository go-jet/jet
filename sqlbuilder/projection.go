package sqlbuilder

type projection interface {
	serializeForProjection(statement statementType, out *queryData) error
}

//------------------------------------------------------//
// Dummy type for select * AllColumns
type ColumnList []Column

func (cl ColumnList) isProjectionType() {}

func (cl ColumnList) serializeForProjection(statement statementType, out *queryData) error {
	projections := columnListToProjectionList(cl)

	err := serializeProjectionList(statement, projections, out)

	if err != nil {
		return err
	}

	return nil
}
