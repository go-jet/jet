package jet

import (
	"errors"
	"github.com/go-jet/jet/internal/utils"
)

type Clause interface {
	Serialize(statementType StatementType, out *SqlBuilder) error
}

type ClauseWithProjections interface {
	Clause

	projections() []Projection
}

type ClauseSelect struct {
	Distinct    bool
	Projections []Projection
}

func (s *ClauseSelect) projections() []Projection {
	return s.Projections
}

func (s *ClauseSelect) Serialize(statementType StatementType, out *SqlBuilder) error {
	out.NewLine()
	out.WriteString("SELECT")

	if s.Distinct {
		out.WriteString("DISTINCT")
	}

	if len(s.Projections) == 0 {
		return errors.New("jet: no column selected for Projection")
	}

	return out.WriteProjections(statementType, s.Projections)
}

type ClauseFrom struct {
	Table Serializer
}

func (f *ClauseFrom) Serialize(statementType StatementType, s *SqlBuilder) error {
	if f.Table == nil {
		return nil
	}
	s.NewLine()
	s.WriteString("FROM")

	s.IncreaseIdent()
	err := f.Table.serialize(statementType, s)
	s.DecreaseIdent()

	return err
}

type ClauseWhere struct {
	Condition BoolExpression
	Mandatory bool
}

func (c *ClauseWhere) Serialize(statementType StatementType, s *SqlBuilder) error {
	if c.Condition == nil {
		if c.Mandatory {
			return errors.New("jet: WHERE clause not set")
		}
		return nil
	}
	s.NewLine()
	s.WriteString("WHERE")

	s.IncreaseIdent()
	err := c.Condition.serialize(statementType, s, noWrap)
	s.DecreaseIdent()

	return err
}

type ClauseGroupBy struct {
	List []GroupByClause
}

func (c *ClauseGroupBy) Serialize(statementType StatementType, out *SqlBuilder) error {
	if len(c.List) == 0 {
		return nil
	}

	out.NewLine()
	out.WriteString("GROUP BY")

	out.IncreaseIdent()
	err := serializeGroupByClauseList(statementType, c.List, out)
	out.DecreaseIdent()

	return err
}

type ClauseHaving struct {
	Condition BoolExpression
}

func (c *ClauseHaving) Serialize(statementType StatementType, s *SqlBuilder) error {
	if c.Condition == nil {
		return nil
	}

	s.NewLine()
	s.WriteString("HAVING")

	s.IncreaseIdent()
	err := c.Condition.serialize(statementType, s, noWrap)
	s.DecreaseIdent()

	return err
}

type ClauseOrderBy struct {
	List []OrderByClause
}

func (o *ClauseOrderBy) Serialize(statementType StatementType, s *SqlBuilder) error {
	if o.List == nil {
		return nil
	}

	s.NewLine()
	s.WriteString("ORDER BY")

	s.IncreaseIdent()
	err := serializeOrderByClauseList(statementType, o.List, s)
	s.DecreaseIdent()

	return err
}

type ClauseLimit struct {
	Count int64
}

func (l *ClauseLimit) Serialize(statementType StatementType, out *SqlBuilder) error {
	if l.Count >= 0 {
		out.NewLine()
		out.WriteString("LIMIT")
		out.insertParametrizedArgument(l.Count)
	}

	return nil
}

type ClauseOffset struct {
	Count int64
}

func (o *ClauseOffset) Serialize(statementType StatementType, out *SqlBuilder) error {
	if o.Count >= 0 {
		out.NewLine()
		out.WriteString("OFFSET")
		out.insertParametrizedArgument(o.Count)
	}

	return nil
}

type ClauseFor struct {
	Lock SelectLock
}

func (f *ClauseFor) Serialize(statementType StatementType, out *SqlBuilder) error {
	if f.Lock == nil {
		return nil
	}

	out.NewLine()
	out.WriteString("FOR")
	return f.Lock.serialize(statementType, out)
}

type ClauseSetStmtOperator struct {
	Operator string
	All      bool
	Selects  []StatementWithProjections
	OrderBy  ClauseOrderBy
	Limit    ClauseLimit
	Offset   ClauseOffset
}

func (s *ClauseSetStmtOperator) projections() []Projection {
	if len(s.Selects) > 0 {
		return s.Selects[0].projections()
	}
	return nil
}

func (s *ClauseSetStmtOperator) Serialize(statementType StatementType, out *SqlBuilder) error {
	if len(s.Selects) < 2 {
		return errors.New("jet: UNION Statement must have at least two SELECT statements")
	}

	wrap := s.OrderBy.List != nil || s.Limit.Count >= 0 || s.Offset.Count >= 0

	if wrap {
		out.NewLine()
		out.WriteString("(")
		out.IncreaseIdent()
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
			return errors.New("jet: select statement is nil")
		}

		err := selectStmt.serialize(statementType, out)

		if err != nil {
			return err
		}
	}

	if wrap {
		out.DecreaseIdent()
		out.NewLine()
		out.WriteString(")")
	}

	if err := s.OrderBy.Serialize(statementType, out); err != nil {
		return err
	}

	if err := s.Limit.Serialize(statementType, out); err != nil {
		return err
	}

	if err := s.Offset.Serialize(statementType, out); err != nil {
		return err
	}

	return nil
}

type ClauseUpdate struct {
	Table SerializerTable
}

func (u *ClauseUpdate) Serialize(statementType StatementType, out *SqlBuilder) error {
	out.NewLine()
	out.WriteString("UPDATE")

	if utils.IsNil(u.Table) {
		return errors.New("jet: table to update is nil")
	}

	if err := u.Table.serialize(statementType, out); err != nil {
		return err
	}

	return nil
}

type ClauseSet struct {
	Columns []Column
	Values  []Serializer
}

func (s *ClauseSet) Serialize(statementType StatementType, out *SqlBuilder) error {
	out.NewLine()
	out.WriteString("SET")

	if len(s.Columns) != len(s.Values) {
		return errors.New("jet: mismatch in numers of columns and values")
	}

	out.IncreaseIdent(4)
	for i, column := range s.Columns {
		if i > 0 {
			out.WriteString(", ")
			out.NewLine()
		}

		if column == nil {
			return errors.New("jet: nil column in columns list")
		}

		out.WriteString(column.Name())

		out.WriteString(" = ")

		if err := s.Values[i].serialize(UpdateStatementType, out); err != nil {
			return err
		}
	}
	out.DecreaseIdent(4)

	return nil
}

type ClauseInsert struct {
	Table   SerializerTable
	Columns []Column
}

func (i *ClauseInsert) GetColumns() []Column {
	if len(i.Columns) > 0 {
		return i.Columns
	}

	return i.Table.Columns()
}

func (i *ClauseInsert) Serialize(statementType StatementType, out *SqlBuilder) error {
	out.NewLine()
	out.WriteString("INSERT INTO")

	if utils.IsNil(i.Table) {
		return errors.New("jet: table is nil")
	}

	err := i.Table.serialize(statementType, out)

	if err != nil {
		return err
	}

	if len(i.Columns) > 0 {
		out.WriteString("(")

		err = SerializeColumnNames(i.Columns, out)

		if err != nil {
			return err
		}

		out.WriteString(")")
	}

	return nil
}

type ClauseValues struct {
	Rows [][]Serializer
}

func (v *ClauseValues) Serialize(statementType StatementType, out *SqlBuilder) error {
	if len(v.Rows) == 0 {
		return nil
	}

	out.WriteString("VALUES")

	for rowIndex, row := range v.Rows {
		if rowIndex > 0 {
			out.WriteString(",")
		}

		out.IncreaseIdent()
		out.NewLine()
		out.WriteString("(")

		err := SerializeClauseList(statementType, row, out)

		if err != nil {
			return err
		}

		out.WriteByte(')')
		out.DecreaseIdent()
	}
	return nil
}

type ClauseQuery struct {
	Query SerializerStatement
}

func (v *ClauseQuery) Serialize(statementType StatementType, out *SqlBuilder) error {
	if v.Query == nil {
		return nil
	}

	return v.Query.serialize(statementType, out)
}

type ClauseDelete struct {
	Table SerializerTable
}

func (d *ClauseDelete) Serialize(statementType StatementType, out *SqlBuilder) error {
	out.NewLine()
	out.WriteString("DELETE FROM")

	if d.Table == nil {
		return errors.New("jet: nil tableName")
	}

	if err := d.Table.serialize(statementType, out); err != nil {
		return err
	}

	return nil
}

type ClauseStatementBegin struct {
	Name   string
	Tables []SerializerTable
}

func (d *ClauseStatementBegin) Serialize(statementType StatementType, out *SqlBuilder) error {
	out.NewLine()
	out.WriteString(d.Name)

	for i, table := range d.Tables {
		if i > 0 {
			out.WriteString(", ")
		}

		err := table.serialize(statementType, out)

		if err != nil {
			return err
		}
	}

	return nil
}

type ClauseString struct {
	Name string
	Data string
}

func (d *ClauseString) Serialize(statementType StatementType, out *SqlBuilder) error {
	out.NewLine()
	out.WriteString(d.Name)
	out.WriteString(d.Data)
	return nil
}

type ClauseOptional struct {
	Name string
	Show bool
}

func (d *ClauseOptional) Serialize(statementType StatementType, out *SqlBuilder) error {
	if !d.Show {
		return nil
	}
	//out.newLine()
	out.WriteString(d.Name)
	return nil
}

type ClauseIn struct {
	LockMode string
}

func (i *ClauseIn) Serialize(statementType StatementType, out *SqlBuilder) error {
	if i.LockMode == "" {
		return nil
	}

	out.WriteString("IN")
	out.WriteString(string(i.LockMode))
	out.WriteString("MODE")

	return nil
}
