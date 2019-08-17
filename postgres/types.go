package postgres

import "github.com/go-jet/jet/internal/jet"

// Statement is common interface for all statements(SELECT, INSERT, UPDATE, DELETE, LOCK)
type Statement jet.Statement

// Projection is interface for all projection types. Types that can be part of, for instance SELECT clause.
type Projection jet.Projection

func toJetProjectionList(projections []Projection) []jet.Projection {
	ret := []jet.Projection{}

	for _, projection := range projections {
		ret = append(ret, projection)
	}

	return ret
}
