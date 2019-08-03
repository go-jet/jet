package jet

type Projection interface {
	serializeForProjection(statement StatementType, out *SqlBuilder) error
	fromImpl(subQuery SelectTable) Projection
}

func SerializeForProjection(projection Projection, statementType StatementType, out *SqlBuilder) error {
	return projection.serializeForProjection(statementType, out)
}

// ProjectionList is a redefined type, so that ProjectionList can be used as a Projection.
type ProjectionList []Projection

func (cl ProjectionList) fromImpl(subQuery SelectTable) Projection {
	newProjectionList := ProjectionList{}

	for _, projection := range cl {
		newProjectionList = append(newProjectionList, projection.fromImpl(subQuery))
	}

	return newProjectionList
}

func (cl ProjectionList) serializeForProjection(statement StatementType, out *SqlBuilder) error {
	err := SerializeProjectionList(statement, cl, out)

	if err != nil {
		return err
	}

	return nil
}
