package postgres

import "github.com/go-jet/jet/internal/jet"

// Statement is common interface for all statements(SELECT, INSERT, UPDATE, DELETE, LOCK)
type Statement = jet.Statement

// Projection is interface for all projection types. Types that can be part of, for instance SELECT clause.
type Projection = jet.Projection

// ProjectionList can be used to create conditional constructed projection list.
type ProjectionList = jet.ProjectionList
