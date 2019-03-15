package sqlbuilder

import (
	"bytes"
)

type Clause interface {
	SerializeSql(out *bytes.Buffer) error
}

// A clause that can be used in order by
type OrderByClause interface {
	Clause
	isOrderByClauseInterface
}

// An expression
type Expression interface {
	Clause
	isExpressionInterface
}

type BoolExpression interface {
	Clause
	isBoolExpressionInterface

	And(expression BoolExpression) BoolExpression
	Or(expression BoolExpression) BoolExpression
}

// A clause that is selectable.
type Projection interface {
	Clause
	isProjectionInterface
	SerializeSqlForColumnList(out *bytes.Buffer) error
}

type ColumnList []NonAliasColumn

func (cl ColumnList) SerializeSql(out *bytes.Buffer) error {
	for i, column := range cl {
		column.SerializeSql(out)

		if i != len(cl)-1 {
			out.WriteString(", ")
		}
	}
	return nil
}

func (cl ColumnList) isProjectionType() {
}

func (cl ColumnList) SerializeSqlForColumnList(out *bytes.Buffer) error {
	for i, column := range cl {
		column.SerializeSqlForColumnList(out)

		if i != len(cl)-1 {
			out.WriteString(", ")
		}
	}
	return nil
}

//
// Boiler plates ...
//

type isOrderByClauseInterface interface {
	isOrderByClauseType()
}

type isOrderByClause struct {
}

func (o *isOrderByClause) isOrderByClauseType() {
}

type isExpressionInterface interface {
	isExpressionType()
}

type isExpression struct {
	isOrderByClause // can always use expression in order by.
}

func (e *isExpression) isExpressionType() {
}

type isBoolExpressionInterface interface {
	isExpressionInterface
	isBoolExpressionType()
}

type isBoolExpression struct {
}

func (e *isBoolExpression) isBoolExpressionType() {
}

type isProjectionInterface interface {
	isProjectionType()
}

type isProjection struct {
}

func (p *isProjection) isProjectionType() {
}
