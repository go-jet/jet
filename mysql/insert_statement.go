package mysql

import "github.com/go-jet/jet/v2/internal/jet"

// InsertStatement is interface for SQL INSERT statements
type InsertStatement interface {
	Statement

	OPTIMIZER_HINTS(hints ...OptimizerHint) InsertStatement

	// Insert row of values
	VALUES(value interface{}, values ...interface{}) InsertStatement
	// Insert row of values, where value for each column is extracted from filed of structure data.
	// If data is not struct or there is no field for every column selected, this method will panic.
	MODEL(data interface{}) InsertStatement
	MODELS(data interface{}) InsertStatement
	AS_NEW() InsertStatement

	ON_DUPLICATE_KEY_UPDATE(assigments ...ColumnAssigment) InsertStatement

	QUERY(selectStatement SelectStatement) InsertStatement
}

func newInsertStatement(table Table, columns []jet.Column) InsertStatement {
	newInsert := &insertStatementImpl{}
	newInsert.SerializerStatement = jet.NewStatementImpl(Dialect, jet.InsertStatementType, newInsert,
		&newInsert.Insert,
		&newInsert.ValuesQuery,
		&newInsert.OnDuplicateKey,
	)

	newInsert.Insert.Table = table
	newInsert.Insert.Columns = columns

	return newInsert
}

type insertStatementImpl struct {
	jet.SerializerStatement

	Insert         jet.ClauseInsert
	ValuesQuery    jet.ClauseValuesQuery
	OnDuplicateKey onDuplicateKeyUpdateClause
}

func (is *insertStatementImpl) OPTIMIZER_HINTS(hints ...OptimizerHint) InsertStatement {
	is.Insert.OptimizerHints = hints
	return is
}

func (is *insertStatementImpl) VALUES(value interface{}, values ...interface{}) InsertStatement {
	is.ValuesQuery.Rows = append(is.ValuesQuery.Rows, jet.UnwindRowFromValues(value, values))
	return is
}

func (is *insertStatementImpl) MODEL(data interface{}) InsertStatement {
	is.ValuesQuery.Rows = append(is.ValuesQuery.Rows, jet.UnwindRowFromModel(is.Insert.GetColumns(), data))
	return is
}

func (is *insertStatementImpl) MODELS(data interface{}) InsertStatement {
	is.ValuesQuery.Rows = append(is.ValuesQuery.Rows, jet.UnwindRowsFromModels(is.Insert.GetColumns(), data)...)
	return is
}

func (is *insertStatementImpl) AS_NEW() InsertStatement {
	is.ValuesQuery.As = "new"
	return is
}

func (is *insertStatementImpl) ON_DUPLICATE_KEY_UPDATE(assigments ...ColumnAssigment) InsertStatement {
	is.OnDuplicateKey = assigments
	return is
}

func (is *insertStatementImpl) QUERY(selectStatement SelectStatement) InsertStatement {
	is.ValuesQuery.Query = selectStatement
	return is
}

type onDuplicateKeyUpdateClause []jet.ColumnAssigment

// Serialize for SetClause
func (s onDuplicateKeyUpdateClause) Serialize(statementType jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
	if len(s) == 0 {
		return
	}
	out.NewLine()
	out.WriteString("ON DUPLICATE KEY UPDATE")
	out.IncreaseIdent(24)

	for i, assigment := range s {
		if i > 0 {
			out.WriteString(",")
			out.NewLine()
		}

		jet.Serialize(assigment, statementType, out, jet.FallTrough(options)...)
	}

	out.DecreaseIdent(24)
}
