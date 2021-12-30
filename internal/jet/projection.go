package jet

import "strings"

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

func (cl ProjectionList) fromImpl(subQuery SelectTable) Projection {
	newProjectionList := ProjectionList{}

	for _, projection := range cl {
		newProjectionList = append(newProjectionList, projection.fromImpl(subQuery))
	}

	return newProjectionList
}

func (cl ProjectionList) serializeForProjection(statement StatementType, out *SQLBuilder) {
	SerializeProjectionList(statement, cl, out)
}

// As is used to set aliases of the projection list. alias should be in the form 'name' or 'name.*'.
// For instance: If projection list has a column 'Artist.Name', and alias is 'Musician.*', returned projection list will
// have column wrapped in alias 'Musician.Name'.
func (cl ProjectionList) As(alias string) ProjectionList {
	alias = strings.TrimRight(alias, ".*")

	newProjectionList := ProjectionList{}

	for _, projection := range cl {
		switch p := projection.(type) {
		case ProjectionList:
			newProjectionList = append(newProjectionList, p.As(alias))
		case ColumnExpression:
			newProjectionList = append(newProjectionList, newAlias(p, alias+"."+p.Name()))
		}
	}

	return newProjectionList
}
