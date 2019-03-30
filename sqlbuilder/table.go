// Modeling of tables.  This is where query preparation starts

package sqlbuilder

import (
	"bytes"
	"fmt"
	"github.com/dropbox/godropbox/errors"
)

// The sql tableName read interface.  NOTE: NATURAL JOINs, and join "USING" clause
// are not supported.
type ReadableTable interface {
	// Returns the list of columns that are in the current tableName expression.
	Columns() []NonAliasColumn

	Column(name string) NonAliasColumn

	// Generates the sql string for the current tableName expression.  Note: the
	// generated string may not be a valid/executable sql statement.
	// The database is the name of the database the tableName is on
	SerializeSql(out *bytes.Buffer) error

	// Generates a select query on the current tableName.
	Select(projections ...Projection) SelectStatement

	// Creates a inner join tableName expression using onCondition.
	InnerJoinOn(table ReadableTable, onCondition BoolExpression) ReadableTable

	InnerJoinUsing(table ReadableTable, col1 Column, col2 Column) ReadableTable

	// Creates a left join tableName expression using onCondition.
	LeftJoinOn(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a right join tableName expression using onCondition.
	RightJoinOn(table ReadableTable, onCondition BoolExpression) ReadableTable

	FullJoin(table ReadableTable, col1 Column, col2 Column) ReadableTable

	CrossJoin(table ReadableTable) ReadableTable
}

// The sql tableName write interface.
type WritableTable interface {
	// Returns the list of columns that are in the tableName.
	Columns() []NonAliasColumn

	// Generates the sql string for the current tableName expression.  Note: the
	// generated string may not be a valid/executable sql statement.
	// The database is the name of the database the tableName is on
	SerializeSql(out *bytes.Buffer) error

	Insert(columns ...NonAliasColumn) InsertStatement
	Update() UpdateStatement
	Delete() DeleteStatement
}

// Defines a physical tableName in the database that is both readable and writable.
// This function will panic if name is not valid
func NewTable(schemaName, name string, columns ...NonAliasColumn) *Table {
	if !validIdentifierName(name) {
		panic("Invalid tableName name")
	}

	t := &Table{
		schemaName:   schemaName,
		name:         name,
		columns:      columns,
		columnLookup: make(map[string]NonAliasColumn),
	}
	for _, c := range columns {
		err := c.setTableName(name)
		if err != nil {
			panic(err)
		}
		t.columnLookup[c.Name()] = c
	}

	if len(columns) == 0 {
		panic(fmt.Sprintf("Table %s has no columns", name))
	}

	return t
}

type Table struct {
	schemaName   string
	name         string
	alias        string
	columns      []NonAliasColumn
	columnLookup map[string]NonAliasColumn
	// If not empty, the name of the index to force
	forcedIndex string
}

// Returns the specified column, or errors if it doesn't exist in the tableName
func (t *Table) getColumn(name string) (NonAliasColumn, error) {
	if c, ok := t.columnLookup[name]; ok {
		return c, nil
	}
	return nil, errors.Newf("No such column '%s' in tableName '%s'", name, t.name)
}

func (t *Table) Column(name string) NonAliasColumn {
	return &baseColumn{
		name:      name,
		nullable:  NotNullable,
		tableName: t.name,
	}
}

// Returns all columns for a tableName as a slice of projections
func (t *Table) Projections() []Projection {
	result := make([]Projection, 0)

	for _, col := range t.columns {
		col.Asc()
		result = append(result, col)
	}

	return result
}

func (t *Table) SetAlias(alias string) {
	t.alias = alias

	for _, c := range t.columns {
		err := c.setTableName(alias)
		if err != nil {
			panic(err)
		}
	}
}

// Returns the tableName's name in the database
func (t *Table) Name() string {
	return t.name
}

func (t *Table) SchemaName() string {
	return t.schemaName
}

// Returns a list of the tableName's columns
func (t *Table) Columns() []NonAliasColumn {
	return t.columns
}

// Returns a copy of this tableName, but with the specified index forced.
func (t *Table) ForceIndex(index string) *Table {
	newTable := *t
	newTable.forcedIndex = index
	return &newTable
}

// Generates the sql string for the current tableName expression.  Note: the
// generated string may not be a valid/executable sql statement.
func (t *Table) SerializeSql(out *bytes.Buffer) error {
	if !validIdentifierName(t.schemaName) {
		return errors.New("Invalid database name specified")
	}

	_, _ = out.WriteString(t.schemaName)
	_, _ = out.WriteString(".")
	_, _ = out.WriteString(t.Name())

	if len(t.alias) > 0 {
		out.WriteString(" AS ")
		out.WriteString(t.alias)
	}

	if t.forcedIndex != "" {
		if !validIdentifierName(t.forcedIndex) {
			return errors.Newf("'%s' is not a valid identifier for an index", t.forcedIndex)
		}
		_, _ = out.WriteString(" FORCE INDEX (")
		_, _ = out.WriteString(t.forcedIndex)
		_, _ = out.WriteString(")")
	}

	return nil
}

// Generates a select query on the current tableName.
func (t *Table) Select(projections ...Projection) SelectStatement {
	return newSelectStatement(t, projections)
}

// Creates a inner join tableName expression using onCondition.
func (t *Table) InnerJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return InnerJoinOn(t, table, onCondition)
}

func (t *Table) InnerJoinUsing(
	table ReadableTable,
	col1 Column,
	col2 Column) ReadableTable {

	return InnerJoinOn(t, table, col1.Eq(col2))
}

// Creates a left join tableName expression using onCondition.
func (t *Table) LeftJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return LeftJoinOn(t, table, onCondition)
}

// Creates a right join tableName expression using onCondition.
func (t *Table) RightJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return RightJoinOn(t, table, onCondition)
}

func (t *Table) FullJoin(table ReadableTable, col1, col2 Column) ReadableTable {
	return FullJoin(t, table, col1.Eq(col2))
}

func (t *Table) CrossJoin(table ReadableTable) ReadableTable {
	return CrossJoin(t, table)
}

func (t *Table) Insert(columns ...NonAliasColumn) InsertStatement {
	return newInsertStatement(t, columns...)
}

func (t *Table) Update() UpdateStatement {
	return newUpdateStatement(t)
}

func (t *Table) Delete() DeleteStatement {
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

func (t *joinTable) Columns() []NonAliasColumn {
	columns := make([]NonAliasColumn, 0)
	columns = append(columns, t.lhs.Columns()...)
	columns = append(columns, t.rhs.Columns()...)

	return columns
}

func (t *joinTable) Column(name string) NonAliasColumn {
	panic("Not implemented")
}

func (t *joinTable) SerializeSql(out *bytes.Buffer) (err error) {

	if t.lhs == nil {
		return errors.Newf("nil lhs.  Generated sql: %s", out.String())
	}
	if t.rhs == nil {
		return errors.Newf("nil rhs.  Generated sql: %s", out.String())
	}
	if t.onCondition == nil && t.join_type != CROSS_JOIN {
		return errors.Newf("nil onCondition.  Generated sql: %s", out.String())
	}

	if err = t.lhs.SerializeSql(out); err != nil {
		return
	}

	switch t.join_type {
	case INNER_JOIN:
		_, _ = out.WriteString(" JOIN ")
	case LEFT_JOIN:
		_, _ = out.WriteString(" LEFT JOIN ")
	case RIGHT_JOIN:
		_, _ = out.WriteString(" RIGHT JOIN ")
	case FULL_JOIN:
		out.WriteString(" FULL JOIN ")
	case CROSS_JOIN:
		out.WriteString(" CROSS JOIN ")
	}

	if err = t.rhs.SerializeSql(out); err != nil {
		return
	}

	if t.onCondition != nil {
		_, _ = out.WriteString(" ON ")
		if err = t.onCondition.SerializeSql(out); err != nil {
			return
		}
	}

	return nil
}

func (t *joinTable) Select(projections ...Projection) SelectStatement {
	return newSelectStatement(t, projections)
}

func (t *joinTable) InnerJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return InnerJoinOn(t, table, onCondition)
}

func (t *joinTable) InnerJoinUsing(
	table ReadableTable,
	col1 Column,
	col2 Column) ReadableTable {

	return InnerJoinOn(t, table, col1.Eq(col2))
}

func (t *joinTable) LeftJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return LeftJoinOn(t, table, onCondition)
}

func (t *joinTable) FullJoin(table ReadableTable, col1 Column, col2 Column) ReadableTable {
	return FullJoin(t, table, col1.Eq(col2))
}

func (t *joinTable) CrossJoin(table ReadableTable) ReadableTable {
	return CrossJoin(t, table)
}

func (t *joinTable) RightJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return RightJoinOn(t, table, onCondition)
}
