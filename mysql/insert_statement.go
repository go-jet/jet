package mysql

import "github.com/go-jet/jet/internal/jet"

// InsertStatement is interface for SQL INSERT statements
type InsertStatement interface {
	jet.Statement

	// Insert row of values
	VALUES(value interface{}, values ...interface{}) InsertStatement
	// Insert row of values, where value for each column is extracted from filed of structure data.
	// If data is not struct or there is no field for every column selected, this method will panic.
	MODEL(data interface{}) InsertStatement
	MODELS(data interface{}) InsertStatement

	QUERY(selectStatement SelectStatement) InsertStatement
}

func newInsertStatement(table Table, columns []jet.Column) InsertStatement {
	newInsert := &insertStatementImpl{}
	newInsert.StatementImpl = jet.NewStatementImpl(Dialect, jet.InsertStatementType, newInsert,
		&newInsert.Insert, &newInsert.Values, &newInsert.Select)

	newInsert.Insert.Table = table
	newInsert.Insert.Columns = columns

	return newInsert
}

type insertStatementImpl struct {
	jet.StatementImpl

	Insert jet.ClauseInsert
	Values jet.ClauseValues
	Select jet.ClauseQuery
}

func (i *insertStatementImpl) VALUES(value interface{}, values ...interface{}) InsertStatement {
	i.Values.Rows = append(i.Values.Rows, jet.UnwindRowFromValues(value, values))
	return i
}

func (i *insertStatementImpl) MODEL(data interface{}) InsertStatement {
	i.Values.Rows = append(i.Values.Rows, jet.UnwindRowFromModel(i.Insert.GetColumns(), data))
	return i
}

func (i *insertStatementImpl) MODELS(data interface{}) InsertStatement {
	i.Values.Rows = append(i.Values.Rows, jet.UnwindRowsFromModels(i.Insert.GetColumns(), data)...)
	return i
}

func (i *insertStatementImpl) QUERY(selectStatement SelectStatement) InsertStatement {
	i.Select.Query = selectStatement
	return i
}
