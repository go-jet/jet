package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

// MergeStatement is interface for CUBRID MERGE INTO statement.
type MergeStatement interface {
	Statement
	USING(source ReadableTable) mergeUsing
}

type mergeUsing interface {
	ON(condition BoolExpression) mergeOn
}

type mergeOn interface {
	WHEN_MATCHED() mergeMatched
	WHEN_NOT_MATCHED() mergeNotMatched
}

type mergeMatched interface {
	THEN_UPDATE(set ...ColumnAssigment) mergeAfterMatched
	THEN_DELETE() mergeAfterMatched
}

type mergeAfterMatched interface {
	Statement
	WHEN_NOT_MATCHED() mergeNotMatched
}

type mergeNotMatched interface {
	THEN_INSERT(columns ...Column) mergeInsertValues
}

type mergeInsertValues interface {
	VALUES(values ...interface{}) Statement
}

// MERGE creates a new MERGE INTO statement for the target table.
func MERGE(target Table) MergeStatement {
	m := &mergeStatementImpl{}
	m.SerializerStatement = jet.NewStatementImpl(Dialect, jet.InsertStatementType, m,
		&m.MergeInto, &m.Using, &m.On, &m.WhenMatched, &m.WhenNotMatched)
	m.MergeInto.Target = target
	return m
}

type mergeStatementImpl struct {
	jet.SerializerStatement
	MergeInto      jet.ClauseMergeInto
	Using          jet.ClauseMergeUsing
	On             jet.ClauseMergeOn
	WhenMatched    jet.ClauseWhenMatched
	WhenNotMatched jet.ClauseWhenNotMatched
}

func (m *mergeStatementImpl) USING(source ReadableTable) mergeUsing {
	m.Using.Source = source
	return m
}

func (m *mergeStatementImpl) ON(condition BoolExpression) mergeOn {
	m.On.Condition = condition
	return m
}

func (m *mergeStatementImpl) WHEN_MATCHED() mergeMatched {
	return &mergeMatchedImpl{merge: m}
}

func (m *mergeStatementImpl) WHEN_NOT_MATCHED() mergeNotMatched {
	return &mergeNotMatchedImpl{merge: m}
}

type mergeMatchedImpl struct{ merge *mergeStatementImpl }

func (mm *mergeMatchedImpl) THEN_UPDATE(sets ...ColumnAssigment) mergeAfterMatched {
	mm.merge.WhenMatched = jet.ClauseWhenMatched{IsUpdate: true, Sets: sets}
	return mm.merge
}

func (mm *mergeMatchedImpl) THEN_DELETE() mergeAfterMatched {
	mm.merge.WhenMatched = jet.ClauseWhenMatched{IsDelete: true}
	return mm.merge
}

type mergeNotMatchedImpl struct{ merge *mergeStatementImpl }

func (mn *mergeNotMatchedImpl) THEN_INSERT(columns ...Column) mergeInsertValues {
	cols := make([]jet.Column, len(columns))
	for i, c := range columns {
		cols[i] = c
	}
	mn.merge.WhenNotMatched.Columns = cols
	return &mergeInsertValuesImpl{merge: mn.merge}
}

type mergeInsertValuesImpl struct{ merge *mergeStatementImpl }

func (mv *mergeInsertValuesImpl) VALUES(values ...interface{}) Statement {
	mv.merge.WhenNotMatched.Values = values
	return mv.merge
}
