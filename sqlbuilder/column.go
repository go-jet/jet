// Modeling of columns

package sqlbuilder

import (
	"strings"
)

type column interface {
	Name() string
	TableName() string

	setTableName(table string)
}

type Column interface {
	Expression
	column
}

// The base type for real materialized columns.
type columnImpl struct {
	expressionInterfaceImpl

	name      string
	tableName string
}

func newColumn(name string, tableName string, parent Column) columnImpl {
	bc := columnImpl{
		name:      name,
		tableName: tableName,
	}

	bc.expressionInterfaceImpl.parent = parent

	return bc
}

func (c *columnImpl) Name() string {
	return c.name
}

func (c *columnImpl) TableName() string {
	return c.tableName
}

func (c *columnImpl) setTableName(table string) {
	c.tableName = table
}

func (c *columnImpl) defaultAlias() string {
	if c.tableName != "" {
		return c.tableName + "." + c.name
	}

	return c.name
}

func (c *columnImpl) serializeForOrderBy(statement statementType, out *queryData) error {
	if statement == set_statement {
		// set Statement (UNION, EXCEPT ...) can reference only select projections in order by clause
		columnRef := ""

		if c.tableName != "" {
			columnRef += c.tableName + "."
		}

		columnRef += c.name

		out.writeString(`"` + columnRef + `"`)

		return nil
	}

	return c.serialize(statement, out)
}

func (c columnImpl) serializeForProjection(statement statementType, out *queryData) error {
	err := c.serialize(statement, out)

	if err != nil {
		return err
	}

	out.writeString(`AS "` + c.defaultAlias() + `"`)

	return nil
}

func (c columnImpl) serialize(statement statementType, out *queryData, options ...serializeOption) error {

	columnRef := ""

	if c.tableName != "" {
		columnRef += c.tableName + "."
	}

	wrapColumnName := strings.Contains(c.name, ".")

	if wrapColumnName {
		columnRef += `"`
	}

	columnRef += c.name

	if wrapColumnName {
		columnRef += `"`
	}

	out.writeString(columnRef)

	return nil
}

//------------------------------------------------------//
// Dummy type for select * AllColumns
type ColumnList []Column

// projection interface implementation
func (cl ColumnList) isProjectionType() {}

func (cl ColumnList) serializeForProjection(statement statementType, out *queryData) error {
	projections := columnListToProjectionList(cl)

	err := serializeProjectionList(statement, projections, out)

	if err != nil {
		return err
	}

	return nil
}

// column interface implementation
func (cl ColumnList) Name() string             { return "" }
func (cl ColumnList) TableName() string        { return "" }
func (cl ColumnList) setTableName(name string) {}
