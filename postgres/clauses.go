package postgres

import (
	"github.com/go-jet/jet/internal/jet"
)

type ClauseReturning struct {
	Projections []jet.Projection
}

func (r *ClauseReturning) Serialize(statementType jet.StatementType, out *jet.SqlBuilder) error {
	if len(r.Projections) == 0 {
		return nil
	}

	out.NewLine()
	out.WriteString("RETURNING")
	out.IncreaseIdent()

	return out.WriteProjections(statementType, r.Projections)
}
