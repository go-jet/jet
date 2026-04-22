package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

// Table is interface for CUBRID tables
type Table interface {
	jet.SerializerTable
	readableTable

	INSERT(columns ...jet.Column) InsertStatement
	UPDATE(columns ...jet.Column) UpdateStatement
	DELETE() DeleteStatement
	LOCK() LockStatement
}

type readableTable interface {
	SELECT(projection Projection, projections ...Projection) SelectStatement
	INNER_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable
	LEFT_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable
	RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable
	FULL_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable
	CROSS_JOIN(table ReadableTable) joinSelectUpdateTable
}

type joinSelectUpdateTable interface {
	ReadableTable
	UPDATE(columns ...jet.Column) UpdateStatement
}

// ReadableTable interface
type ReadableTable interface {
	readableTable
	jet.Serializer
}

type readableTableInterfaceImpl struct {
	root ReadableTable
}

func (r readableTableInterfaceImpl) SELECT(projection1 Projection, projections ...Projection) SelectStatement {
	return newSelectStatement(jet.SelectStatementType, r.root, append([]Projection{projection1}, projections...))
}
func (r readableTableInterfaceImpl) INNER_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable {
	return newJoinTable(r.root, table, jet.InnerJoin, onCondition)
}
func (r readableTableInterfaceImpl) LEFT_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable {
	return newJoinTable(r.root, table, jet.LeftJoin, onCondition)
}
func (r readableTableInterfaceImpl) RIGHT_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable {
	return newJoinTable(r.root, table, jet.RightJoin, onCondition)
}
func (r readableTableInterfaceImpl) FULL_JOIN(table ReadableTable, onCondition BoolExpression) joinSelectUpdateTable {
	return newJoinTable(r.root, table, jet.FullJoin, onCondition)
}
func (r readableTableInterfaceImpl) CROSS_JOIN(table ReadableTable) joinSelectUpdateTable {
	return newJoinTable(r.root, table, jet.CrossJoin, nil)
}

// NewTable creates new table with schema Name, table Name and list of columns
func NewTable(schemaName, name, alias string, columns ...jet.ColumnExpression) Table {
	t := &tableImpl{SerializerTable: jet.NewTable(schemaName, name, alias, columns...)}
	t.readableTableInterfaceImpl.root = t
	t.root = t
	return t
}

type tableImpl struct {
	jet.SerializerTable
	readableTableInterfaceImpl
	root Table
}

func (t *tableImpl) INSERT(columns ...jet.Column) InsertStatement {
	return newInsertStatement(t.root, jet.UnwidColumnList(columns))
}
func (t *tableImpl) UPDATE(columns ...jet.Column) UpdateStatement {
	return newUpdateStatement(t.root, jet.UnwidColumnList(columns))
}
func (t *tableImpl) DELETE() DeleteStatement {
	return newDeleteStatement(t.root)
}
func (t *tableImpl) LOCK() LockStatement {
	return LOCK(t.root)
}

type joinTable struct {
	tableImpl
	jet.JoinTable
}

func newJoinTable(lhs jet.Serializer, rhs jet.Serializer, joinType jet.JoinType, onCondition BoolExpression) Table {
	newJT := &joinTable{JoinTable: jet.NewJoinTable(lhs, rhs, joinType, onCondition)}
	newJT.readableTableInterfaceImpl.root = newJT
	newJT.root = newJT
	return newJT
}
