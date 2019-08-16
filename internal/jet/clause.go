package jet

import (
	"github.com/go-jet/jet/internal/utils"
)

type Clause interface {
	Serialize(statementType StatementType, out *SqlBuilder)
}

type ClauseWithProjections interface {
	Clause

	projections() ProjectionList
}

type ClauseSelect struct {
	Distinct    bool
	Projections []Projection
}

func (s *ClauseSelect) projections() ProjectionList {
	return s.Projections
}

func (s *ClauseSelect) Serialize(statementType StatementType, out *SqlBuilder) {
	out.NewLine()
	out.WriteString("SELECT")

	if s.Distinct {
		out.WriteString("DISTINCT")
	}

	if len(s.Projections) == 0 {
		panic("jet: SELECT clause has to have at least one projection")
	}

	out.WriteProjections(statementType, s.Projections)
}

type ClauseFrom struct {
	Table Serializer
}

func (f *ClauseFrom) Serialize(statementType StatementType, out *SqlBuilder) {
	if f.Table == nil {
		return
	}
	out.NewLine()
	out.WriteString("FROM")

	out.IncreaseIdent()
	f.Table.serialize(statementType, out)
	out.DecreaseIdent()
}

type ClauseWhere struct {
	Condition BoolExpression
	Mandatory bool
}

func (c *ClauseWhere) Serialize(statementType StatementType, out *SqlBuilder) {
	if c.Condition == nil {
		if c.Mandatory {
			panic("jet: WHERE clause not set")
		}
		return
	}
	out.NewLine()
	out.WriteString("WHERE")

	out.IncreaseIdent()
	c.Condition.serialize(statementType, out, noWrap)
	out.DecreaseIdent()
}

type ClauseGroupBy struct {
	List []GroupByClause
}

func (c *ClauseGroupBy) Serialize(statementType StatementType, out *SqlBuilder) {
	if len(c.List) == 0 {
		return
	}

	out.NewLine()
	out.WriteString("GROUP BY")

	out.IncreaseIdent()
	serializeGroupByClauseList(statementType, c.List, out)
	out.DecreaseIdent()
}

type ClauseHaving struct {
	Condition BoolExpression
}

func (c *ClauseHaving) Serialize(statementType StatementType, out *SqlBuilder) {
	if c.Condition == nil {
		return
	}

	out.NewLine()
	out.WriteString("HAVING")

	out.IncreaseIdent()
	c.Condition.serialize(statementType, out, noWrap)
	out.DecreaseIdent()
}

type ClauseOrderBy struct {
	List []OrderByClause
}

func (o *ClauseOrderBy) Serialize(statementType StatementType, out *SqlBuilder) {
	if o.List == nil {
		return
	}

	out.NewLine()
	out.WriteString("ORDER BY")

	out.IncreaseIdent()
	serializeOrderByClauseList(statementType, o.List, out)
	out.DecreaseIdent()
}

type ClauseLimit struct {
	Count int64
}

func (l *ClauseLimit) Serialize(statementType StatementType, out *SqlBuilder) {
	if l.Count >= 0 {
		out.NewLine()
		out.WriteString("LIMIT")
		out.insertParametrizedArgument(l.Count)
	}
}

type ClauseOffset struct {
	Count int64
}

func (o *ClauseOffset) Serialize(statementType StatementType, out *SqlBuilder) {
	if o.Count >= 0 {
		out.NewLine()
		out.WriteString("OFFSET")
		out.insertParametrizedArgument(o.Count)
	}
}

type ClauseFor struct {
	Lock SelectLock
}

func (f *ClauseFor) Serialize(statementType StatementType, out *SqlBuilder) {
	if f.Lock == nil {
		return
	}

	out.NewLine()
	out.WriteString("FOR")
	f.Lock.serialize(statementType, out)
}

type ClauseSetStmtOperator struct {
	Operator string
	All      bool
	Selects  []StatementWithProjections
	OrderBy  ClauseOrderBy
	Limit    ClauseLimit
	Offset   ClauseOffset
}

func (s *ClauseSetStmtOperator) projections() ProjectionList {
	if len(s.Selects) > 0 {
		return s.Selects[0].projections()
	}
	return nil
}

func (s *ClauseSetStmtOperator) Serialize(statementType StatementType, out *SqlBuilder) {
	if len(s.Selects) < 2 {
		panic("jet: UNION Statement must contain at least two SELECT statements")
	}

	for i, selectStmt := range s.Selects {
		out.NewLine()
		if i > 0 {
			out.WriteString(s.Operator)

			if s.All {
				out.WriteString("ALL")
			}
			out.NewLine()
		}

		if selectStmt == nil {
			panic("jet: select statement of '" + s.Operator + "' is nil")
		}

		selectStmt.serialize(statementType, out)
	}

	s.OrderBy.Serialize(statementType, out)
	s.Limit.Serialize(statementType, out)
	s.Offset.Serialize(statementType, out)
}

type ClauseUpdate struct {
	Table SerializerTable
}

func (u *ClauseUpdate) Serialize(statementType StatementType, out *SqlBuilder) {
	out.NewLine()
	out.WriteString("UPDATE")

	if utils.IsNil(u.Table) {
		panic("jet: table to update is nil")
	}

	u.Table.serialize(statementType, out)
}

type ClauseSet struct {
	Columns []Column
	Values  []Serializer
}

func (s *ClauseSet) Serialize(statementType StatementType, out *SqlBuilder) {
	out.NewLine()
	out.WriteString("SET")

	if len(s.Columns) != len(s.Values) {
		panic("jet: mismatch in numbers of columns and values for SET clause")
	}

	out.IncreaseIdent(4)
	for i, column := range s.Columns {
		if i > 0 {
			out.WriteString(", ")
			out.NewLine()
		}

		if column == nil {
			panic("jet: nil column in columns list for SET clause")
		}

		out.WriteString(column.Name())

		out.WriteString(" = ")

		s.Values[i].serialize(UpdateStatementType, out)
	}
	out.DecreaseIdent(4)
}

type ClauseInsert struct {
	Table   SerializerTable
	Columns []Column
}

func (i *ClauseInsert) GetColumns() []Column {
	if len(i.Columns) > 0 {
		return i.Columns
	}

	return i.Table.columns()
}

func (i *ClauseInsert) Serialize(statementType StatementType, out *SqlBuilder) {
	out.NewLine()
	out.WriteString("INSERT INTO")

	if utils.IsNil(i.Table) {
		panic("jet: table is nil for INSERT clause")
	}

	i.Table.serialize(statementType, out)

	if len(i.Columns) > 0 {
		out.WriteString("(")

		SerializeColumnNames(i.Columns, out)

		out.WriteString(")")
	}
}

type ClauseValuesQuery struct {
	ClauseValues
	ClauseQuery
}

func (v *ClauseValuesQuery) Serialize(statementType StatementType, out *SqlBuilder) {
	if len(v.Rows) == 0 && v.Query == nil {
		panic("jet: VALUES or QUERY has to be specified for INSERT statement")
	}

	if len(v.Rows) > 0 && v.Query != nil {
		panic("jet: VALUES or QUERY has to be specified for INSERT statement")
	}

	v.ClauseValues.Serialize(statementType, out)
	v.ClauseQuery.Serialize(statementType, out)
}

type ClauseValues struct {
	Rows [][]Serializer
}

func (v *ClauseValues) Serialize(statementType StatementType, out *SqlBuilder) {
	if len(v.Rows) == 0 {
		return
	}

	out.WriteString("VALUES")

	for rowIndex, row := range v.Rows {
		if rowIndex > 0 {
			out.WriteString(",")
		}

		out.IncreaseIdent()
		out.NewLine()
		out.WriteString("(")

		SerializeClauseList(statementType, row, out)

		out.WriteByte(')')
		out.DecreaseIdent()
	}
}

type ClauseQuery struct {
	Query SerializerStatement
}

func (v *ClauseQuery) Serialize(statementType StatementType, out *SqlBuilder) {
	if v.Query == nil {
		return
	}

	v.Query.serialize(statementType, out)
}

type ClauseDelete struct {
	Table SerializerTable
}

func (d *ClauseDelete) Serialize(statementType StatementType, out *SqlBuilder) {
	out.NewLine()
	out.WriteString("DELETE FROM")

	if d.Table == nil {
		panic("jet: nil table in DELETE clause")
	}

	d.Table.serialize(statementType, out)
}

type ClauseStatementBegin struct {
	Name   string
	Tables []SerializerTable
}

func (d *ClauseStatementBegin) Serialize(statementType StatementType, out *SqlBuilder) {
	out.NewLine()
	out.WriteString(d.Name)

	for i, table := range d.Tables {
		if i > 0 {
			out.WriteString(", ")
		}

		table.serialize(statementType, out)
	}
}

type ClauseOptional struct {
	Name      string
	Show      bool
	InNewLine bool
}

func (d *ClauseOptional) Serialize(statementType StatementType, out *SqlBuilder) {
	if !d.Show {
		return
	}
	if d.InNewLine {
		out.NewLine()
	}
	out.WriteString(d.Name)
}

type ClauseIn struct {
	LockMode string
}

func (i *ClauseIn) Serialize(statementType StatementType, out *SqlBuilder) {
	if i.LockMode == "" {
		return
	}

	out.WriteString("IN")
	out.WriteString(string(i.LockMode))
	out.WriteString("MODE")
}
