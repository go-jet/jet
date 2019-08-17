package postgres

import (
	"github.com/go-jet/jet/internal/jet"
)

type clauseReturning struct {
	Projections []jet.Projection
}

func (r *clauseReturning) Serialize(statementType jet.StatementType, out *jet.SqlBuilder) {
	if len(r.Projections) == 0 {
		return
	}

	out.NewLine()
	out.WriteString("RETURNING")
	out.IncreaseIdent()
	out.WriteProjections(statementType, r.Projections)
}
