package jet

type projection interface {
	serializeForProjection(statement statementType, out *queryData) error
	from(subQuery ExpressionTable) projection
}

//------------------------------------------------------//
// Dummy type for projection list
type ProjectionList []projection

func (cl ProjectionList) from(subQuery ExpressionTable) projection {
	newProjectionList := ProjectionList{}

	for _, projection := range cl {
		newProjectionList = append(newProjectionList, projection.from(subQuery))
	}

	return newProjectionList
}

func (cl ProjectionList) serializeForProjection(statement statementType, out *queryData) error {
	err := serializeProjectionList(statement, cl, out)

	if err != nil {
		return err
	}

	return nil
}
