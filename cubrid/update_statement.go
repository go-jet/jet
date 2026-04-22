package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

// UpdateStatement is interface of SQL UPDATE statement
type UpdateStatement interface {
	jet.Statement
	SET(value interface{}, values ...interface{}) UpdateStatement
	MODEL(data interface{}) UpdateStatement
	WHERE(expression BoolExpression) UpdateStatement
	LIMIT(limit int64) UpdateStatement
}

func newUpdateStatement(table Table, columns []jet.Column) UpdateStatement {
	u := &updateStatementImpl{}
	u.SerializerStatement = jet.NewStatementImpl(Dialect, jet.UpdateStatementType, u,
		&u.Update, &u.Set, &u.SetNew, &u.Where, &u.Limit)
	u.Update.Table = table
	u.Set.Columns = columns
	u.Where.Mandatory = true
	u.Limit.Count = -1
	return u
}

type updateStatementImpl struct {
	jet.SerializerStatement
	Update jet.ClauseUpdate
	Set    jet.SetClause
	SetNew jet.SetClauseNew
	Where  jet.ClauseWhere
	Limit  jet.ClauseLimit
}

func (u *updateStatementImpl) SET(v interface{}, vs ...interface{}) UpdateStatement {
	ca, ok := v.(ColumnAssigment)
	if ok {
		u.SetNew = []ColumnAssigment{ca}
		for _, val := range vs {
			u.SetNew = append(u.SetNew, val.(ColumnAssigment))
		}
	} else {
		u.Set.Values = jet.UnwindRowFromValues(v, vs)
	}
	return u
}
func (u *updateStatementImpl) MODEL(data interface{}) UpdateStatement {
	u.Set.Values = jet.UnwindRowFromModel(u.Set.Columns, data); return u
}
func (u *updateStatementImpl) WHERE(e BoolExpression) UpdateStatement { u.Where.Condition = e; return u }
func (u *updateStatementImpl) LIMIT(l int64) UpdateStatement          { u.Limit.Count = l; return u }
