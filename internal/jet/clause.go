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

	return out.writeProjections(statementType, s.Projections)
}

type ClauseFrom struct {
	Table Serializer
}

func (f *ClauseFrom) Serialize(statementType StatementType, out *SqlBuilder) error {
	if f.Table == nil {
		return nil
	}
	return out.writeFrom(statementType, f.Table)
}

type ClauseWhere struct {
	Condition BoolExpression
	Mandatory bool
}

func (c *ClauseWhere) Serialize(statementType StatementType, out *SqlBuilder) error {
	if c.Condition == nil {
		if c.Mandatory {
			return errors.New("jet: WHERE clause not set")
		}
		return nil
	}
	return out.writeWhere(statementType, c.Condition)
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

	out.increaseIdent()
	err := serializeGroupByClauseList(statementType, c.List, out)
	out.decreaseIdent()

	return err
}

type ClauseHaving struct {
	Condition BoolExpression
}

func (c *ClauseHaving) Serialize(statementType StatementType, out *SqlBuilder) error {
	if c.Condition == nil {
		return nil
	}

	return out.writeHaving(statementType, c.Condition)
}

type ClauseOrderBy struct {
	List []OrderByClause
}

func (o *ClauseOrderBy) Serialize(statementType StatementType, out *SqlBuilder) error {
	if o.List == nil {
		return nil
	}

	return out.writeOrderBy(statementType, o.List)
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

	//if wrap {
	//	out.WriteString("(")
	//	out.increaseIdent()
	//}

	if wrap {
		out.NewLine()
		out.WriteString("(")
		out.increaseIdent()
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
		out.decreaseIdent()
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

	//if wrap {
	//	out.decreaseIdent()
	//	out.newLine()
	//	out.WriteString(")")
	//}

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
	Columns []IColumn
	Values  []Serializer
}

func (s *ClauseSet) Serialize(statementType StatementType, out *SqlBuilder) error {
	out.NewLine()
	out.WriteString("SET")

	if len(s.Columns) != len(s.Values) {
		return errors.New("jet: mismatch in numers of columns and values")
	}

	out.increaseIdent(4)
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

		if err := Serialize(s.Values[i], UpdateStatementType, out); err != nil {
			return err
		}
	}
	out.decreaseIdent(4)

	return nil
}

type ClauseReturning struct {
	Projections []Projection
}

func (r *ClauseReturning) Serialize(statementType StatementType, out *SqlBuilder) error {
	return out.WriteReturning(statementType, r.Projections)
}

type ClauseInsert struct {
	Table   SerializerTable
	Columns []IColumn
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

		out.increaseIdent()
		out.NewLine()
		out.WriteString("(")

		err := SerializeClauseList(statementType, row, out)

		if err != nil {
			return err
		}

		out.writeByte(')')
		out.decreaseIdent()
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

// NewTable creates new table with schema Name, table Name and list of columns
func NewTable2(Dialect Dialect, schemaName, name string, columns ...Column) TableImpl2 {

	t := TableImpl2{
		Dialect:    Dialect,
		schemaName: schemaName,
		name:       name,
		columnList: columns,
	}

	for _, c := range columns {
		c.SetTableName(name)
	}

	return t
}

type TableImpl2 struct {
	Dialect    Dialect
	schemaName string
	name       string
	alias      string
	columnList []Column
}

func (t *TableImpl2) AS(alias string) {
	t.alias = alias

	for _, c := range t.columnList {
		c.SetTableName(alias)
	}
}

func (t *TableImpl2) SchemaName() string {
	return t.schemaName
}

func (t *TableImpl2) TableName() string {
	return t.name
}

func (t *TableImpl2) Columns() []IColumn {
	ret := []IColumn{}

	for _, col := range t.columnList {
		ret = append(ret, col)
	}

	return ret
}

func (t *TableImpl2) dialect() Dialect {
	return t.Dialect
}

func (t *TableImpl2) accept(visitor visitor) {
	visitor.visit(t)
}

func (t *TableImpl2) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	if t == nil {
		return errors.New("jet: tableImpl is nil. ")
	}

	out.writeIdentifier(t.schemaName)
	out.WriteString(".")
	out.writeIdentifier(t.name)

	if len(t.alias) > 0 {
		out.WriteString("AS")
		out.writeIdentifier(t.alias)
	}

	return nil
}

// Join expressions are pseudo readable tables.
type JoinTableImpl struct {
	lhs         Serializer
	rhs         Serializer
	joinType    JoinType
	onCondition BoolExpression
}

func NewJoinTableImpl(lhs Serializer, rhs Serializer, joinType JoinType, onCondition BoolExpression) JoinTableImpl {

	joinTable := JoinTableImpl{
		lhs:         lhs,
		rhs:         rhs,
		joinType:    joinType,
		onCondition: onCondition,
	}

	return joinTable
}

func (t *JoinTableImpl) SchemaName() string {
	return ""
}

func (t *JoinTableImpl) TableName() string {
	return ""
}

func (t *JoinTableImpl) Columns() []IColumn {
	//return append(t.lhs.columns(), t.rhs.columns()...)
	panic("Unimplemented")
}

func (t *JoinTableImpl) accept(visitor visitor) {
	//t.lhs.accept(visitor)
	//t.rhs.accept(visitor)
	//TODO: uncoment
}

func (t *JoinTableImpl) dialect() Dialect {
	return detectDialect(t)
}

func (t *JoinTableImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) (err error) {
	if t == nil {
		return errors.New("jet: Join table is nil. ")
	}

	if utils.IsNil(t.lhs) {
		return errors.New("jet: left hand side of join operation is nil table")
	}

	if err = t.lhs.serialize(statement, out); err != nil {
		return
	}

	out.NewLine()

	switch t.joinType {
	case InnerJoin:
		out.WriteString("INNER JOIN")
	case LeftJoin:
		out.WriteString("LEFT JOIN")
	case RightJoin:
		out.WriteString("RIGHT JOIN")
	case FullJoin:
		out.WriteString("FULL JOIN")
	case CrossJoin:
		out.WriteString("CROSS JOIN")
	}

	if utils.IsNil(t.rhs) {
		return errors.New("jet: right hand side of join operation is nil table")
	}

	if err = t.rhs.serialize(statement, out); err != nil {
		return
	}

	if t.onCondition == nil && t.joinType != CrossJoin {
		return errors.New("jet: join condition is nil")
	}

	if t.onCondition != nil {
		out.WriteString("ON")
		if err = t.onCondition.serialize(statement, out); err != nil {
			return
		}
	}

	return nil
}

// SelectTable is interface for SELECT sub-queries
type SelectTable interface {
	Alias() string
	AllColumns() ProjectionList
}

type SelectTableImpl2 struct {
	selectStmt StatementWithProjections
	alias      string

	projections []Projection
}

func NewSelectTable(selectStmt StatementWithProjections, alias string) SelectTableImpl2 {
	selectTable := SelectTableImpl2{selectStmt: selectStmt, alias: alias}

	for _, projection := range selectStmt.projections() {
		newProjection := projection.fromImpl(&selectTable)

		selectTable.projections = append(selectTable.projections, newProjection)
	}

	return selectTable
}

func (s *SelectTableImpl2) Alias() string {
	return s.alias
}

func (s *SelectTableImpl2) Columns() []IColumn {
	return nil
}

func (s *SelectTableImpl2) accept(visitor visitor) {
	visitor.visit(s)
	s.selectStmt.accept(visitor)
}

func (s *SelectTableImpl2) dialect() Dialect {
	return detectDialect(s.selectStmt)
}

func (s *SelectTableImpl2) AllColumns() ProjectionList {
	return s.projections
}

func (s *SelectTableImpl2) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	if s == nil {
		return errors.New("jet: Expression table is nil. ")
	}

	err := s.selectStmt.serialize(statement, out)

	if err != nil {
		return err
	}

	out.WriteString("AS")
	out.writeIdentifier(s.alias)

	return nil
}
