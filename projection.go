package jet

type projection interface {
	serializeForProjection(statement statementType, out *sqlBuilder) error
	from(subQuery SelectTable) projection
}

// ProjectionList is a redefined type, so that ProjectionList can be used as a projection.
type ProjectionList []projection

func (cl ProjectionList) from(subQuery SelectTable) projection {
	newProjectionList := ProjectionList{}

	for _, projection := range cl {
		newProjectionList = append(newProjectionList, projection.from(subQuery))
	}

	return newProjectionList
}

func (cl ProjectionList) serializeForProjection(statement statementType, out *sqlBuilder) error {
	err := serializeProjectionList(statement, cl, out)

	if err != nil {
		return err
	}

	return nil
}
