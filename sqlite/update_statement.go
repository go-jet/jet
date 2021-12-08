package sqlite

import "github.com/go-jet/jet/v2/internal/jet"

// UpdateStatement is interface of SQL UPDATE statement
type UpdateStatement interface {
	jet.Statement

	SET(value interface{}, values ...interface{}) UpdateStatement
	MODEL(data interface{}) UpdateStatement

	FROM(tables ...ReadableTable) UpdateStatement
	WHERE(expression BoolExpression) UpdateStatement
	RETURNING(projections ...Projection) UpdateStatement
}

type updateStatementImpl struct {
	jet.SerializerStatement

	Update    jet.ClauseUpdate
	From      jet.ClauseFrom
	Set       jet.SetClause
	SetNew    jet.SetClauseNew
	Where     jet.ClauseWhere
	Returning jet.ClauseReturning
}

func newUpdateStatement(table Table, columns []jet.Column) UpdateStatement {
	update := &updateStatementImpl{}
	update.SerializerStatement = jet.NewStatementImpl(Dialect, jet.UpdateStatementType, update,
		&update.Update,
		&update.Set,
		&update.SetNew,
		&update.From,
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

func (u *updateStatementImpl) FROM(tables ...ReadableTable) UpdateStatement {
	u.From.Tables = readableTablesToSerializerList(tables)
	return u
}

func (u *updateStatementImpl) WHERE(expression BoolExpression) UpdateStatement {
	u.Where.Condition = expression
	return u
}

func (u *updateStatementImpl) RETURNING(projections ...Projection) UpdateStatement {
	u.Returning.ProjectionList = projections
	return u
}
