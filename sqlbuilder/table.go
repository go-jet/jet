// Modeling of tables.  This is where query preparation starts

package sqlbuilder

import (
	"github.com/dropbox/godropbox/errors"
)

type tableInterface interface {
	clause
	SchemaName() string
	TableName() string

	Columns() []column
}

// The sql tableName read interface.  NOTE: NATURAL JOINs, and join "USING" clause
// are not supported.
type readableTable interface {
	tableInterface

	// Generates a select query on the current tableName.
	SELECT(projections ...projection) selectStatement

	// Creates a inner join tableName expression using onCondition.
	INNER_JOIN(table readableTable, onCondition boolExpression) readableTable

	// Creates a left join tableName expression using onCondition.
	LEFT_JOIN(table readableTable, onCondition boolExpression) readableTable

	// Creates a right join tableName expression using onCondition.
	RIGHT_JOIN(table readableTable, onCondition boolExpression) readableTable

	FULL_JOIN(table readableTable, onCondition boolExpression) readableTable

	CROSS_JOIN(table readableTable) readableTable
}

// The sql tableName write interface.
type writableTable interface {
	tableInterface

	INSERT(columns ...column) insertStatement
	UPDATE(columns ...column) updateStatement
	DELETE() deleteStatement

	LOCK() lockStatement
}

// Defines a physical tableName in the database that is both readable and writable.
// This function will panic if name is not valid
func NewTable(schemaName, name string, columns ...column) *Table {

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
	columns    []column
}

func (t *Table) Column(name string) column {
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
func (t *Table) Columns() []column {
	return t.columns
}

// Generates the sql string for the current tableName expression.  Note: the
// generated string may not be a valid/executable sql Statement.
func (t *Table) serialize(statement statementType, out *queryData) error {
	if t == nil {
		return errors.Newf("nil tableName.")
	}

	out.writeString(t.schemaName)
	out.writeString(".")
	out.writeString(t.TableName())

	if len(t.alias) > 0 {
		out.writeString(" AS ")
		out.writeString(t.alias)
	}

	return nil
}

// Generates a select query on the current tableName.
func (t *Table) SELECT(projections ...projection) selectStatement {
	return newSelectStatement(t, projections)
}

// Creates a inner join tableName expression using onCondition.
func (t *Table) INNER_JOIN(
	table readableTable,
	onCondition boolExpression) readableTable {

	return InnerJoinOn(t, table, onCondition)
}

// Creates a left join tableName expression using onCondition.
func (t *Table) LEFT_JOIN(
	table readableTable,
	onCondition boolExpression) readableTable {

	return LeftJoinOn(t, table, onCondition)
}

// Creates a right join tableName expression using onCondition.
func (t *Table) RIGHT_JOIN(
	table readableTable,
	onCondition boolExpression) readableTable {

	return RightJoinOn(t, table, onCondition)
}

func (t *Table) FULL_JOIN(table readableTable, onCondition boolExpression) readableTable {
	return FullJoin(t, table, onCondition)
}

func (t *Table) CROSS_JOIN(table readableTable) readableTable {
	return CrossJoin(t, table)
}

func (t *Table) INSERT(columns ...column) insertStatement {
	return newInsertStatement(t, columns...)
}

func (t *Table) UPDATE(columns ...column) updateStatement {
	return newUpdateStatement(t, columns)
}

func (t *Table) DELETE() deleteStatement {
	return newDeleteStatement(t)
}

func (t *Table) LOCK() lockStatement {
	return LOCK(t)
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
	lhs         readableTable
	rhs         readableTable
	join_type   joinType
	onCondition boolExpression
}

func newJoinTable(
	lhs readableTable,
	rhs readableTable,
	join_type joinType,
	onCondition boolExpression) readableTable {

	return &joinTable{
		lhs:         lhs,
		rhs:         rhs,
		join_type:   join_type,
		onCondition: onCondition,
	}
}

func InnerJoinOn(
	lhs readableTable,
	rhs readableTable,
	onCondition boolExpression) readableTable {

	return newJoinTable(lhs, rhs, INNER_JOIN, onCondition)
}

func LeftJoinOn(
	lhs readableTable,
	rhs readableTable,
	onCondition boolExpression) readableTable {

	return newJoinTable(lhs, rhs, LEFT_JOIN, onCondition)
}

func RightJoinOn(
	lhs readableTable,
	rhs readableTable,
	onCondition boolExpression) readableTable {

	return newJoinTable(lhs, rhs, RIGHT_JOIN, onCondition)
}

func FullJoin(
	lhs readableTable,
	rhs readableTable,
	onCondition boolExpression) readableTable {

	return newJoinTable(lhs, rhs, FULL_JOIN, onCondition)
}

func CrossJoin(
	lhs readableTable,
	rhs readableTable) readableTable {

	return newJoinTable(lhs, rhs, CROSS_JOIN, nil)
}

func (t *joinTable) SchemaName() string {
	return ""
}

func (t *joinTable) TableName() string {
	return ""
}

func (t *joinTable) Columns() []column {
	columns := make([]column, 0)
	columns = append(columns, t.lhs.Columns()...)
	columns = append(columns, t.rhs.Columns()...)

	return columns
}

func (t *joinTable) Column(name string) column {
	return &baseColumn{
		name:     name,
		nullable: NotNullable,
	}
}

func (t *joinTable) serialize(statement statementType, out *queryData) (err error) {

	if t.lhs == nil {
		return errors.Newf("nil lhs.")
	}
	if t.rhs == nil {
		return errors.Newf("nil rhs.")
	}
	if t.onCondition == nil && t.join_type != CROSS_JOIN {
		return errors.Newf("nil onCondition.")
	}

	if err = t.lhs.serialize(statement, out); err != nil {
		return
	}

	out.nextLine()

	switch t.join_type {
	case INNER_JOIN:
		out.writeString("JOIN")
	case LEFT_JOIN:
		out.writeString("LEFT JOIN")
	case RIGHT_JOIN:
		out.writeString("RIGHT JOIN")
	case FULL_JOIN:
		out.writeString("FULL JOIN")
	case CROSS_JOIN:
		out.writeString("CROSS JOIN")
	}

	if err = t.rhs.serialize(statement, out); err != nil {
		return
	}

	if t.onCondition != nil {
		out.writeString(" ON ")
		if err = t.onCondition.serialize(statement, out); err != nil {
			return
		}
	}

	return nil
}

func (t *joinTable) SELECT(projections ...projection) selectStatement {
	return newSelectStatement(t, projections)
}

func (t *joinTable) INNER_JOIN(
	table readableTable,
	onCondition boolExpression) readableTable {

	return InnerJoinOn(t, table, onCondition)
}

func (t *joinTable) LEFT_JOIN(
	table readableTable,
	onCondition boolExpression) readableTable {

	return LeftJoinOn(t, table, onCondition)
}

func (t *joinTable) FULL_JOIN(table readableTable, onCondition boolExpression) readableTable {
	return FullJoin(t, table, onCondition)
}

func (t *joinTable) CROSS_JOIN(table readableTable) readableTable {
	return CrossJoin(t, table)
}

func (t *joinTable) RIGHT_JOIN(
	table readableTable,
	onCondition boolExpression) readableTable {

	return RightJoinOn(t, table, onCondition)
}
