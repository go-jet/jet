// Modeling of columns

package sqlbuilder

import (
	"strings"
)

type Column interface {
	Expression

	Name() string
	TableName() string
	IsNullable() bool
	DefaultAlias() projection
	// Internal function for tracking tableName that a column belongs to
	// for the purpose of serialization
	setTableName(table string)
}

// The base type for real materialized columns.
type baseColumn struct {
	expressionInterfaceImpl

	name       string
	isNullable bool
	tableName  string
}

func newBaseColumn(name string, isNullable bool, tableName string, parent Column) baseColumn {
	bc := baseColumn{
		name:       name,
		isNullable: isNullable,
		tableName:  tableName,
	}

	bc.expressionInterfaceImpl.parent = parent

	return bc
}

func (c *baseColumn) Name() string {
	return c.name
}

func (c *baseColumn) TableName() string {
	return c.tableName
}

func (c *baseColumn) setTableName(table string) {
	c.tableName = table
}

func (c *baseColumn) IsNullable() bool {
	return c.isNullable
}

func (c *baseColumn) DefaultAlias() projection {
	return c.AS(c.tableName + "." + c.name)
}

func (c *baseColumn) serializeAsOrderBy(statement statementType, out *queryData) error {
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

func (c baseColumn) serialize(statement statementType, out *queryData, options ...serializeOption) error {

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
