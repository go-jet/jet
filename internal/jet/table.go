package jet

import (
	"errors"
	"github.com/go-jet/jet/internal/utils"
)

type table interface {
	dialect() Dialect
	columns() []IColumn
}

type readableTable interface {
	// Generates a select query on the current tableName.
	SELECT(projection Projection, projections ...Projection) SelectStatement

	// Creates a inner join tableName Expression using onCondition.
	INNER_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a left join tableName Expression using onCondition.
	LEFT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a right join tableName Expression using onCondition.
	RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a full join tableName Expression using onCondition.
	FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a cross join tableName Expression using onCondition.
	CROSS_JOIN(table ReadableTable) ReadableTable
}

type writableTable interface {
	INSERT(columns ...IColumn) InsertStatement
	UPDATE(column IColumn, columns ...IColumn) UpdateStatement
	DELETE() DeleteStatement

	LOCK() LockStatement
}

// ReadableTable interface
type ReadableTable interface {
	table
	readableTable
	Clause
	acceptsVisitor
}

// WritableTable interface
type WritableTable interface {
	table
	writableTable
	Clause
	acceptsVisitor
}

// Table interface
type Table interface {
	table
	readableTable
	writableTable
	Clause
	acceptsVisitor

	SchemaName() string
	TableName() string
	AS(alias string)
}

type readableTableInterfaceImpl struct {
	parent ReadableTable
}

// Generates a select query on the current tableName.
func (r *readableTableInterfaceImpl) SELECT(projection1 Projection, projections ...Projection) SelectStatement {
	return newSelectStatement(r.parent, append([]Projection{projection1}, projections...))
}

// Creates a inner join tableName Expression using onCondition.
func (r *readableTableInterfaceImpl) INNER_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, innerJoin, onCondition)
}

// Creates a left join tableName Expression using onCondition.
func (r *readableTableInterfaceImpl) LEFT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, leftJoin, onCondition)
}

// Creates a right join tableName Expression using onCondition.
func (r *readableTableInterfaceImpl) RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, rightJoin, onCondition)
}

func (r *readableTableInterfaceImpl) FULL_JOIN(table ReadableTable, onCondition BoolExpression) ReadableTable {
	return newJoinTable(r.parent, table, fullJoin, onCondition)
}

func (r *readableTableInterfaceImpl) CROSS_JOIN(table ReadableTable) ReadableTable {
	return newJoinTable(r.parent, table, crossJoin, nil)
}

type writableTableInterfaceImpl struct {
	parent WritableTable
}

func (w *writableTableInterfaceImpl) INSERT(columns ...IColumn) InsertStatement {
	return newInsertStatement(w.parent, unwidColumnList(columns))
}

func (w *writableTableInterfaceImpl) UPDATE(column IColumn, columns ...IColumn) UpdateStatement {
	return newUpdateStatement(w.parent, unwindColumns(column, columns...))
}

func (w *writableTableInterfaceImpl) DELETE() DeleteStatement {
	return newDeleteStatement(w.parent)
}

func (w *writableTableInterfaceImpl) LOCK() LockStatement {
	return LOCK(w.parent)
}

// NewTable creates new table with schema Name, table Name and list of columns
func NewTable(Dialect Dialect, schemaName, name string, columns ...Column) Table {

	t := &tableImpl{
		Dialect:    Dialect,
		schemaName: schemaName,
		name:       name,
		columnList: columns,
	}
	for _, c := range columns {
		c.SetTableName(name)
	}

	t.readableTableInterfaceImpl.parent = t
	t.writableTableInterfaceImpl.parent = t

	return t
}

type tableImpl struct {
	readableTableInterfaceImpl
	writableTableInterfaceImpl

	Dialect    Dialect
	schemaName string
	name       string
	alias      string
	columnList []Column
}

func (t *tableImpl) AS(alias string) {
	t.alias = alias

	for _, c := range t.columnList {
		c.SetTableName(alias)
	}
}

func (t *tableImpl) SchemaName() string {
	return t.schemaName
}

func (t *tableImpl) TableName() string {
	return t.name
}

func (t *tableImpl) columns() []IColumn {
	ret := []IColumn{}

	for _, col := range t.columnList {
		ret = append(ret, col)
	}

	return ret
}

func (t *tableImpl) dialect() Dialect {
	return t.Dialect
}

func (t *tableImpl) accept(visitor visitor) {
	visitor.visit(t)
}

func (t *tableImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
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

type joinType int

const (
	innerJoin joinType = iota
	leftJoin
	rightJoin
	fullJoin
	crossJoin
)

// Join expressions are pseudo readable tables.
type joinTable struct {
	readableTableInterfaceImpl

	lhs         ReadableTable
	rhs         ReadableTable
	joinType    joinType
	onCondition BoolExpression
}

func newJoinTable(
	lhs ReadableTable,
	rhs ReadableTable,
	joinType joinType,
	onCondition BoolExpression) ReadableTable {

	joinTable := &joinTable{
		lhs:         lhs,
		rhs:         rhs,
		joinType:    joinType,
		onCondition: onCondition,
	}

	joinTable.readableTableInterfaceImpl.parent = joinTable

	return joinTable
}

func (t *joinTable) SchemaName() string {
	return ""
}

func (t *joinTable) TableName() string {
	return ""
}

func (t *joinTable) columns() []IColumn {
	return append(t.lhs.columns(), t.rhs.columns()...)
}

func (t *joinTable) accept(visitor visitor) {
	t.lhs.accept(visitor)
	t.rhs.accept(visitor)
}

func (t *joinTable) dialect() Dialect {
	return detectDialect(t)
}

func (t *joinTable) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) (err error) {
	if t == nil {
		return errors.New("jet: Join table is nil. ")
	}

	if utils.IsNil(t.lhs) {
		return errors.New("jet: left hand side of join operation is nil table")
	}

	if err = t.lhs.serialize(statement, out); err != nil {
		return
	}

	out.newLine()

	switch t.joinType {
	case innerJoin:
		out.WriteString("INNER JOIN")
	case leftJoin:
		out.WriteString("LEFT JOIN")
	case rightJoin:
		out.WriteString("RIGHT JOIN")
	case fullJoin:
		out.WriteString("FULL JOIN")
	case crossJoin:
		out.WriteString("CROSS JOIN")
	}

	if utils.IsNil(t.rhs) {
		return errors.New("jet: right hand side of join operation is nil table")
	}

	if err = t.rhs.serialize(statement, out); err != nil {
		return
	}

	if t.onCondition == nil && t.joinType != crossJoin {
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

func unwindColumns(column1 IColumn, columns ...IColumn) []IColumn {
	columnList := []IColumn{}

	if val, ok := column1.(IColumnList); ok {
		for _, col := range val.Columns() {
			columnList = append(columnList, col)
		}
		columnList = append(columnList, columns...)
	} else {
		columnList = append(columnList, column1)
		columnList = append(columnList, columns...)
	}

	return columnList
}

func unwidColumnList(columns []IColumn) []IColumn {
	ret := []IColumn{}

	for _, col := range columns {
		if columnList, ok := col.(IColumnList); ok {
			for _, c := range columnList.Columns() {
				ret = append(ret, c)
			}
		} else {
			ret = append(ret, col)
		}
	}

	return ret
}
