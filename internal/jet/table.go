package jet

import (
	"github.com/go-jet/jet/internal/utils"
)

type SerializerTable interface {
	Serializer
	TableInterface
}

type TableInterface interface {
	columns() []Column
	SchemaName() string
	TableName() string
	AS(alias string)
}

// NewTable creates new table with schema Name, table Name and list of columns
func NewTable(schemaName, name string, columns ...ColumnExpression) TableImpl {

	t := TableImpl{
		schemaName: schemaName,
		name:       name,
		columnList: columns,
	}

	for _, c := range columns {
		c.setTableName(name)
	}

	return t
}

type TableImpl struct {
	schemaName string
	name       string
	alias      string
	columnList []ColumnExpression
}

func (t *TableImpl) AS(alias string) {
	t.alias = alias

	for _, c := range t.columnList {
		c.setTableName(alias)
	}
}

func (t *TableImpl) SchemaName() string {
	return t.schemaName
}

func (t *TableImpl) TableName() string {
	return t.name
}

func (t *TableImpl) columns() []Column {
	ret := []Column{}

	for _, col := range t.columnList {
		ret = append(ret, col)
	}

	return ret
}

func (t *TableImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) {
	if t == nil {
		panic("jet: tableImpl is nil")
	}

	out.WriteIdentifier(t.schemaName)
	out.WriteString(".")
	out.WriteIdentifier(t.name)

	if len(t.alias) > 0 {
		out.WriteString("AS")
		out.WriteIdentifier(t.alias)
	}
}

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
	CrossJoin
)

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
	if table, ok := t.lhs.(TableInterface); ok {
		return table.SchemaName()
	}
	return ""
}

func (t *JoinTableImpl) TableName() string {
	return ""
}

func (t *JoinTableImpl) Columns() []Column {
	var ret []Column

	if lhsTable, ok := t.lhs.(TableInterface); ok {
		ret = append(ret, lhsTable.columns()...)
	}
	if rhsTable, ok := t.rhs.(TableInterface); ok {
		ret = append(ret, rhsTable.columns()...)
	}

	return ret
}

func (t *JoinTableImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) {
	if t == nil {
		panic("jet: Join table is nil. ")
	}

	if utils.IsNil(t.lhs) {
		panic("jet: left hand side of join operation is nil table")
	}

	t.lhs.serialize(statement, out)

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
		panic("jet: right hand side of join operation is nil table")
	}

	t.rhs.serialize(statement, out)

	if t.onCondition == nil && t.joinType != CrossJoin {
		panic("jet: join condition is nil")
	}

	if t.onCondition != nil {
		out.WriteString("ON")
		t.onCondition.serialize(statement, out)
	}
}

func UnwindColumns(column1 Column, columns ...Column) []Column {
	columnList := []Column{}

	if val, ok := column1.(IColumnList); ok {
		for _, col := range val.columns() {
			columnList = append(columnList, col)
		}
		columnList = append(columnList, columns...)
	} else {
		columnList = append(columnList, column1)
		columnList = append(columnList, columns...)
	}

	return columnList
}

func UnwidColumnList(columns []Column) []Column {
	ret := []Column{}

	for _, col := range columns {
		if columnList, ok := col.(IColumnList); ok {
			for _, c := range columnList.columns() {
				ret = append(ret, c)
			}
		} else {
			ret = append(ret, col)
		}
	}

	return ret
}
