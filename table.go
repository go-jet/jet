// Modeling of tables.  This is where query preparation starts

package jet

import (
	"errors"
)

type table interface {
	columns() []column
}

type readableTable interface {
	// Generates a select query on the current tableName.
	SELECT(projection projection, projections ...projection) SelectStatement

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

// The sql tableName write interface.
type writableTable interface {
	INSERT(columns ...column) InsertStatement
	UPDATE(column column, columns ...column) UpdateStatement
	DELETE() DeleteStatement

	LOCK() LockStatement
}

type ReadableTable interface {
	table
	readableTable
	clause
}

type WritableTable interface {
	table
	writableTable
	clause
}

type Table interface {
	table
	readableTable
	writableTable
	clause
	SchemaName() string
	TableName() string
	AS(alias string)
}

type readableTableInterfaceImpl struct {
	parent ReadableTable
}

// Generates a select query on the current tableName.
func (r *readableTableInterfaceImpl) SELECT(projection1 projection, projections ...projection) SelectStatement {
	return newSelectStatement(r.parent, append([]projection{projection1}, projections...))
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

func (w *writableTableInterfaceImpl) INSERT(columns ...column) InsertStatement {
	return newInsertStatement(w.parent, unwidColumnList(columns))
}

func (w *writableTableInterfaceImpl) UPDATE(column column, columns ...column) UpdateStatement {
	return newUpdateStatement(w.parent, unwindColumns(column, columns...))
}

func (w *writableTableInterfaceImpl) DELETE() DeleteStatement {
	return newDeleteStatement(w.parent)
}

func (w *writableTableInterfaceImpl) LOCK() LockStatement {
	return LOCK(w.parent)
}

func NewTable(schemaName, name string, columns ...Column) Table {

	t := &tableImpl{
		schemaName: schemaName,
		name:       name,
		columnList: columns,
	}
	for _, c := range columns {
		c.setTableName(name)
	}

	t.readableTableInterfaceImpl.parent = t
	t.writableTableInterfaceImpl.parent = t

	return t
}

type tableImpl struct {
	readableTableInterfaceImpl
	writableTableInterfaceImpl

	schemaName string
	name       string
	alias      string
	columnList []Column
}

func (t *tableImpl) AS(alias string) {
	t.alias = alias

	for _, c := range t.columnList {
		c.setTableName(alias)
	}
}

// Returns the tableName's name in the database
func (t *tableImpl) SchemaName() string {
	return t.schemaName
}

// Returns the tableName's name in the database
func (t *tableImpl) TableName() string {
	return t.name
}

func (t *tableImpl) columns() []column {
	ret := []column{}

	for _, col := range t.columnList {
		ret = append(ret, col)
	}

	return ret
}

func (t *tableImpl) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	if t == nil {
		return errors.New("tableImpl is nil. ")
	}

	out.writeIdentifier(t.schemaName)
	out.writeString(".")
	out.writeIdentifier(t.name)

	if len(t.alias) > 0 {
		out.writeString("AS")
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
	join_type   joinType
	onCondition BoolExpression
}

func newJoinTable(
	lhs ReadableTable,
	rhs ReadableTable,
	join_type joinType,
	onCondition BoolExpression) ReadableTable {

	joinTable := &joinTable{
		lhs:         lhs,
		rhs:         rhs,
		join_type:   join_type,
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

func (t *joinTable) columns() []column {
	return append(t.lhs.columns(), t.rhs.columns()...)
}

func (t *joinTable) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) (err error) {
	if t == nil {
		return errors.New("Join table is nil. ")
	}

	if isNil(t.lhs) {
		return errors.New("left hand side of join operation is nil table")
	}

	if err = t.lhs.serialize(statement, out); err != nil {
		return
	}

	out.newLine()

	switch t.join_type {
	case innerJoin:
		out.writeString("INNER JOIN")
	case leftJoin:
		out.writeString("LEFT JOIN")
	case rightJoin:
		out.writeString("RIGHT JOIN")
	case fullJoin:
		out.writeString("FULL JOIN")
	case crossJoin:
		out.writeString("CROSS JOIN")
	}

	if isNil(t.rhs) {
		return errors.New("right hand side of join operation is nil table")
	}

	if err = t.rhs.serialize(statement, out); err != nil {
		return
	}

	if t.onCondition == nil && t.join_type != crossJoin {
		return errors.New("join condition is nil")
	}

	if t.onCondition != nil {
		out.writeString("ON")
		if err = t.onCondition.serialize(statement, out); err != nil {
			return
		}
	}

	return nil
}

func unwindColumns(column1 column, columns ...column) []column {
	columnList := []column{}

	if val, ok := column1.(ColumnList); ok {
		for _, col := range val {
			columnList = append(columnList, col)
		}
		columnList = append(columnList, columns...)
	} else {
		columnList = append(columnList, column1)
		columnList = append(columnList, columns...)
	}

	return columnList
}

func unwidColumnList(columns []column) []column {
	ret := []column{}

	for _, col := range columns {
		if columnList, ok := col.(ColumnList); ok {
			for _, c := range columnList {
				ret = append(ret, c)
			}
		} else {
			ret = append(ret, col)
		}
	}

	return ret
}
