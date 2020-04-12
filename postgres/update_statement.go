package postgres

import (
	"github.com/go-jet/jet/internal/jet"
)

// UpdateStatement is interface of SQL UPDATE statement
type UpdateStatement interface {
	Statement

	SET(value interface{}, values ...interface{}) UpdateStatement
	MODEL(data interface{}) UpdateStatement

	WHERE(expression BoolExpression) UpdateStatement
	RETURNING(projections ...jet.Projection) UpdateStatement
}

type updateStatementImpl struct {
	jet.SerializerStatement

	Update    jet.ClauseUpdate
	Set       clauseSet
	Where     jet.ClauseWhere
	Returning clauseReturning
}

func newUpdateStatement(table WritableTable, columns []jet.Column) UpdateStatement {
	update := &updateStatementImpl{}
	update.SerializerStatement = jet.NewStatementImpl(Dialect, jet.UpdateStatementType, update, &update.Update,
		&update.Set, &update.Where, &update.Returning)

	update.Update.Table = table
	update.Set.Columns = columns
	update.Where.Mandatory = true

	return update
}

func (u *updateStatementImpl) SET(value interface{}, values ...interface{}) UpdateStatement {
	u.Set.Values = jet.UnwindRowFromValues(value, values)
	return u
}

func (u *updateStatementImpl) MODEL(data interface{}) UpdateStatement {
	u.Set.Values = jet.UnwindRowFromModel(u.Set.Columns, data)
	return u
}

func (u *updateStatementImpl) WHERE(expression BoolExpression) UpdateStatement {
	u.Where.Condition = expression
	return u
}

func (u *updateStatementImpl) RETURNING(projections ...jet.Projection) UpdateStatement {
	u.Returning.Projections = projections
	return u
}

type clauseSet struct {
	Columns []jet.Column
	Values  []jet.Serializer
}

func (s *clauseSet) Serialize(statementType jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
	out.NewLine()
	out.WriteString("SET")

	if len(s.Columns) == 0 {
		panic("jet: no columns selected")
	}

	if len(s.Columns) > 1 {
		out.WriteString("(")
	}

	jet.SerializeColumnNames(s.Columns, out)

	if len(s.Columns) > 1 {
		out.WriteString(")")
	}

	out.WriteString("=")

	if len(s.Values) > 1 {
		out.WriteString("(")
	}

	jet.SerializeClauseList(statementType, s.Values, out)

	if len(s.Values) > 1 {
		out.WriteString(")")
	}
}
