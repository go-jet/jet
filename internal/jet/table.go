package jet

import (
	"errors"
	"github.com/go-jet/jet/internal/utils"
)

type SerializerTable interface {
	Serializer
	TableInterface
}

type TableInterface interface {
	Columns() []Column
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
		c.SetTableName(name)
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
		c.SetTableName(alias)
	}
}

func (t *TableImpl) SchemaName() string {
	return t.schemaName
}

func (t *TableImpl) TableName() string {
	return t.name
}

func (t *TableImpl) Columns() []Column {
	ret := []Column{}

	for _, col := range t.columnList {
		ret = append(ret, col)
	}

	return ret
}

func (t *TableImpl) accept(visitor visitor) {
	visitor.visit(t)
}

func (t *TableImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
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
	return ""
}

func (t *JoinTableImpl) TableName() string {
	return ""
}

func (t *JoinTableImpl) Columns() []Column {
	//return append(t.lhs.columns(), t.rhs.columns()...)
	panic("Unimplemented")
}

func (t *JoinTableImpl) accept(visitor visitor) {
	//t.lhs.accept(visitor)
	//t.rhs.accept(visitor)
	//TODO: remove
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

func UnwindColumns(column1 Column, columns ...Column) []Column {
	columnList := []Column{}

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

func UnwidColumnList(columns []Column) []Column {
	ret := []Column{}

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
