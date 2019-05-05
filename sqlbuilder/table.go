// Modeling of tables.  This is where query preparation starts

package sqlbuilder

import (
	"github.com/dropbox/godropbox/errors"
)

type tableInterface interface {
	SchemaName() string
	TableName() string

	Columns() []Column
	// Generates the sql string for the current tableName expression.
	SerializeSql(out *queryData) error
}

// The sql tableName read interface.  NOTE: NATURAL JOINs, and join "USING" clause
// are not supported.
type ReadableTable interface {
	tableInterface

	// Generates a select query on the current tableName.
	SELECT(projections ...Projection) SelectStatement

	// Creates a inner join tableName expression using onCondition.
	INNER_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a left join tableName expression using onCondition.
	LEFT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a right join tableName expression using onCondition.
	RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	CROSS_JOIN(table ReadableTable) ReadableTable
}

// The sql tableName write interface.
type WritableTable interface {
	tableInterface

	INSERT(columns ...Column) InsertStatement
	UPDATE(columns ...Column) UpdateStatement
	DELETE() DeleteStatement
}

// Defines a physical tableName in the database that is both readable and writable.
// This function will panic if name is not valid
func NewTable(schemaName, name string, columns ...Column) *Table {

	t := &Table{
		schemaName: schemaName,
		name:       name,
		columns:    columns,
	}
	for _, c := range columns {
		c.setTableName(name)
	}

	return t
}

type Table struct {
	schemaName string
	name       string
	alias      string
	columns    []Column
}

func (t *Table) Column(name string) Column {
	return &baseColumn{
		name:      name,
		nullable:  NotNullable,
		tableName: t.name,
	}
}

func (t *Table) SetAlias(alias string) {
	t.alias = alias

	for _, c := range t.columns {
		c.setTableName(alias)
	}
}

// Returns the tableName's name in the database
func (t *Table) SchemaName() string {
	return t.schemaName
}

// Returns the tableName's name in the database
func (t *Table) TableName() string {
	return t.name
}

func (t *Table) SchemaTableName() string {
	return t.schemaName
}

// Returns a list of the tableName's columns
func (t *Table) Columns() []Column {
	return t.columns
}

// Generates the sql string for the current tableName expression.  Note: the
// generated string may not be a valid/executable sql statement.
func (t *Table) SerializeSql(out *queryData) error {
	if t == nil {
		return errors.Newf("nil tableName.")
	}

	out.WriteString(t.schemaName)
	out.WriteString(".")
	out.WriteString(t.TableName())

	if len(t.alias) > 0 {
		out.WriteString(" AS ")
		out.WriteString(t.alias)
	}

	return nil
}

// Generates a select query on the current tableName.
func (t *Table) SELECT(projections ...Projection) SelectStatement {
	return newSelectStatement(t, projections)
}

// Creates a inner join tableName expression using onCondition.
func (t *Table) INNER_JOIN(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return InnerJoinOn(t, table, onCondition)
}

// Creates a left join tableName expression using onCondition.
func (t *Table) LEFT_JOIN(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return LeftJoinOn(t, table, onCondition)
}

// Creates a right join tableName expression using onCondition.
func (t *Table) RIGHT_JOIN(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return RightJoinOn(t, table, onCondition)
}

func (t *Table) FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return FullJoin(t, table, onCondition)
}

func (t *Table) CROSS_JOIN(table ReadableTable) ReadableTable {
	return CrossJoin(t, table)
}

func (t *Table) INSERT(columns ...Column) InsertStatement {
	return newInsertStatement(t, columns...)
}

func (t *Table) UPDATE(columns ...Column) UpdateStatement {
	return newUpdateStatement(t, columns)
}

func (t *Table) DELETE() DeleteStatement {
	return newDeleteStatement(t)
}

type joinType int

const (
	INNER_JOIN joinType = iota
	LEFT_JOIN
	RIGHT_JOIN
	FULL_JOIN
	CROSS_JOIN
)

// Join expressions are pseudo readable tables.
type joinTable struct {
	lhs         ReadableTable
	rhs         ReadableTable
	join_type   joinType
	onCondition BoolExpression
}

func newJoinTable(
	lhs ReadableTable,
	rhs ReadableTable,
	join_type joinType,
	onCondition BoolExpression) ReadableTable {

	return &joinTable{
		lhs:         lhs,
		rhs:         rhs,
		join_type:   join_type,
		onCondition: onCondition,
	}
}

func InnerJoinOn(
	lhs ReadableTable,
	rhs ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return newJoinTable(lhs, rhs, INNER_JOIN, onCondition)
}

func LeftJoinOn(
	lhs ReadableTable,
	rhs ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return newJoinTable(lhs, rhs, LEFT_JOIN, onCondition)
}

func RightJoinOn(
	lhs ReadableTable,
	rhs ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return newJoinTable(lhs, rhs, RIGHT_JOIN, onCondition)
}

func FullJoin(
	lhs ReadableTable,
	rhs ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return newJoinTable(lhs, rhs, FULL_JOIN, onCondition)
}

func CrossJoin(
	lhs ReadableTable,
	rhs ReadableTable) ReadableTable {

	return newJoinTable(lhs, rhs, CROSS_JOIN, nil)
}

func (t *joinTable) SchemaName() string {
	return ""
}

func (t *joinTable) TableName() string {
	return ""
}

func (t *joinTable) Columns() []Column {
	columns := make([]Column, 0)
	columns = append(columns, t.lhs.Columns()...)
	columns = append(columns, t.rhs.Columns()...)

	return columns
}

func (t *joinTable) Column(name string) Column {
	return &baseColumn{
		name:     name,
		nullable: NotNullable,
	}
}

func (t *joinTable) SerializeSql(out *queryData) (err error) {

	if t.lhs == nil {
		return errors.Newf("nil lhs.")
	}
	if t.rhs == nil {
		return errors.Newf("nil rhs.")
	}
	if t.onCondition == nil && t.join_type != CROSS_JOIN {
		return errors.Newf("nil onCondition.")
	}

	if err = t.lhs.SerializeSql(out); err != nil {
		return
	}

	switch t.join_type {
	case INNER_JOIN:
		out.WriteString(" JOIN ")
	case LEFT_JOIN:
		out.WriteString(" LEFT JOIN ")
	case RIGHT_JOIN:
		out.WriteString(" RIGHT JOIN ")
	case FULL_JOIN:
		out.WriteString(" FULL JOIN ")
	case CROSS_JOIN:
		out.WriteString(" CROSS JOIN ")
	}

	if err = t.rhs.SerializeSql(out); err != nil {
		return
	}

	if t.onCondition != nil {
		out.WriteString(" ON ")
		if err = t.onCondition.Serialize(out); err != nil {
			return
		}
	}

	return nil
}

func (t *joinTable) SELECT(projections ...Projection) SelectStatement {
	return newSelectStatement(t, projections)
}

func (t *joinTable) INNER_JOIN(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return InnerJoinOn(t, table, onCondition)
}

func (t *joinTable) LEFT_JOIN(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return LeftJoinOn(t, table, onCondition)
}

func (t *joinTable) FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return FullJoin(t, table, onCondition)
}

func (t *joinTable) CROSS_JOIN(table ReadableTable) ReadableTable {
	return CrossJoin(t, table)
}

func (t *joinTable) RIGHT_JOIN(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return RightJoinOn(t, table, onCondition)
}
