package postgres

import (
	"github.com/go-jet/jet/v2/internal/jet"
)

// UpdateStatement is interface of SQL UPDATE statement
type UpdateStatement interface {
	jet.SerializerStatement

	SET(value interface{}, values ...interface{}) UpdateStatement
	MODEL(data interface{}) UpdateStatement

	WHERE(expression BoolExpression) UpdateStatement
	RETURNING(projections ...jet.Projection) UpdateStatement
}

type updateStatementImpl struct {
	jet.SerializerStatement

	Update    jet.ClauseUpdate
	Set       clauseSet
	SetNew    jet.SetClauseNew
	Where     jet.ClauseWhere
	Returning clauseReturning
}

func newUpdateStatement(table WritableTable, columns []jet.Column) UpdateStatement {
	update := &updateStatementImpl{}
	update.SerializerStatement = jet.NewStatementImpl(Dialect, jet.UpdateStatementType, update,
		&update.Update,
		&update.Set,
		&update.SetNew,
		&update.Where,
		&update.Returning)

	update.Update.Table = table
	update.Set.Columns = columns
	update.Where.Mandatory = true

	return update
}

func (u *updateStatementImpl) SET(value interface{}, values ...interface{}) UpdateStatement {
	columnAssigment, isColumnAssigment := value.(ColumnAssigment)

	if isColumnAssigment {
		u.SetNew = []ColumnAssigment{columnAssigment}
		for _, value := range values {
			u.SetNew = append(u.SetNew, value.(ColumnAssigment))
		}
	} else {
		u.Set.Values = jet.UnwindRowFromValues(value, values)
	}

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
	u.Returning.ProjectionList = projections
	return u
}

type clauseSet struct {
	Columns []jet.Column
	Values  []jet.Serializer
}

func (s *clauseSet) Serialize(statementType jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
	if len(s.Values) == 0 {
		return
	}
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
