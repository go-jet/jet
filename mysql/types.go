package mysql

import "github.com/go-jet/jet/internal/jet"

type Statement jet.Statement
type Projection jet.Projection

func toJetProjectionList(projections []Projection) []jet.Projection {
	ret := []jet.Projection{}

	for _, projection := range projections {
		ret = append(ret, projection)
	}

	return ret
}
