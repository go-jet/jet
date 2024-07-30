package postgres

import (
	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/internal/utils/is"
)

type onConflict interface {
	ON_CONSTRAINT(name string) conflictTarget
	WHERE(indexPredicate BoolExpression) conflictTarget
	conflictTarget
}

type conflictTarget interface {
	DO_NOTHING() InsertStatement
	DO_UPDATE(action conflictAction) InsertStatement
}

type onConflictClause struct {
	insertStatement  InsertStatement
	constraint       string
	indexExpressions []jet.ColumnExpression
	whereClause      jet.ClauseWhere
	do               jet.Serializer
}

func (o *onConflictClause) ON_CONSTRAINT(name string) conflictTarget {
	o.constraint = name
	return o
}

func (o *onConflictClause) WHERE(indexPredicate BoolExpression) conflictTarget {
	o.whereClause.Condition = indexPredicate
	return o
}

func (o *onConflictClause) DO_NOTHING() InsertStatement {
	o.do = jet.Keyword("DO NOTHING")
	return o.insertStatement
}

func (o *onConflictClause) DO_UPDATE(action conflictAction) InsertStatement {
	o.do = action
	return o.insertStatement
}

func (o *onConflictClause) Serialize(statementType jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
	if is.Nil(o.do) {
		return
	}

	out.NewLine()
	out.WriteString("ON CONFLICT")
	if len(o.indexExpressions) > 0 {
		out.WriteString("(")
		jet.SerializeColumnExpressions(o.indexExpressions, statementType, out, jet.ShortName)
		out.WriteString(")")
	}

	if o.constraint != "" {
		out.WriteString("ON CONSTRAINT")
		out.WriteString(o.constraint)
	}

	o.whereClause.Serialize(statementType, out, jet.SkipNewLine, jet.ShortName)

	out.IncreaseIdent(7)
	jet.Serialize(o.do, statementType, out)
	out.DecreaseIdent(7)
}
