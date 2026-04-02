package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

// InsertStatement is interface for SQL INSERT statements
type InsertStatement interface {
	Statement
	VALUES(value interface{}, values ...interface{}) InsertStatement
	MODEL(data interface{}) InsertStatement
	MODELS(data interface{}) InsertStatement
	ON_DUPLICATE_KEY_UPDATE(assigments ...ColumnAssigment) InsertStatement
	QUERY(selectStatement SelectStatement) InsertStatement
}

func newInsertStatement(table Table, columns []jet.Column) InsertStatement {
	ins := &insertStatementImpl{}
	ins.SerializerStatement = jet.NewStatementImpl(Dialect, jet.InsertStatementType, ins,
		&ins.Insert, &ins.ValuesQuery, &ins.OnDuplicateKey)
	ins.Insert.Table = table
	ins.Insert.Columns = columns
	return ins
}

type insertStatementImpl struct {
	jet.SerializerStatement
	Insert         jet.ClauseInsert
	ValuesQuery    jet.ClauseValuesQuery
	OnDuplicateKey onDuplicateKeyUpdateClause
}

func (is *insertStatementImpl) VALUES(v interface{}, vs ...interface{}) InsertStatement {
	is.ValuesQuery.Rows = append(is.ValuesQuery.Rows, jet.UnwindRowFromValues(v, vs)); return is
}
func (is *insertStatementImpl) MODEL(data interface{}) InsertStatement {
	is.ValuesQuery.Rows = append(is.ValuesQuery.Rows, jet.UnwindRowFromModel(is.Insert.GetColumns(), data)); return is
}
func (is *insertStatementImpl) MODELS(data interface{}) InsertStatement {
	is.ValuesQuery.Rows = append(is.ValuesQuery.Rows, jet.UnwindRowsFromModels(is.Insert.GetColumns(), data)...); return is
}
func (is *insertStatementImpl) ON_DUPLICATE_KEY_UPDATE(a ...ColumnAssigment) InsertStatement {
	is.OnDuplicateKey = a; return is
}
func (is *insertStatementImpl) QUERY(s SelectStatement) InsertStatement {
	is.ValuesQuery.Query = s; return is
}

type onDuplicateKeyUpdateClause []jet.ColumnAssigment

func (s onDuplicateKeyUpdateClause) Serialize(statementType jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
	if len(s) == 0 {
		return
	}
	out.NewLine()
	out.WriteString("ON DUPLICATE KEY UPDATE")
	out.IncreaseIdent(24)
	for i, a := range s {
		if i > 0 {
			out.WriteString(",")
			out.NewLine()
		}
		jet.Serialize(a, statementType, out, jet.FallTrough(options)...)
	}
	out.DecreaseIdent(24)
}
