package jet

// Projection is interface for all projection types. Types that can be part of, for instance SELECT clause.
type Projection interface {
	serializeForProjection(statement StatementType, out *SqlBuilder)
	fromImpl(subQuery SelectTable) Projection
}

func SerializeForProjection(projection Projection, statementType StatementType, out *SqlBuilder) {
	projection.serializeForProjection(statementType, out)
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

func (cl ProjectionList) serializeForProjection(statement StatementType, out *SqlBuilder) {
	SerializeProjectionList(statement, cl, out)
}
