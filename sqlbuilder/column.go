// Modeling of columns

package sqlbuilder

import (
	"strings"
)

type column interface {
	expression

	Name() string
	TableName() string

	DefaultAlias() projection
	// Internal function for tracking tableName that a column belongs to
	// for the purpose of serialization
	setTableName(table string)
}

type NullableColumn bool

const (
	Nullable    NullableColumn = true
	NotNullable NullableColumn = false
)

type Collation string

const (
	UTF8CaseInsensitive Collation = "utf8_unicode_ci"
	UTF8CaseSensitive   Collation = "utf8_unicode"
	UTF8Binary          Collation = "utf8_bin"
)

// Representation of MySQL charsets
type Charset string

const (
	UTF8 Charset = "utf8"
)

// The base type for real materialized columns.
type baseColumn struct {
	expressionInterfaceImpl

	name      string
	nullable  NullableColumn
	tableName string
}

func newBaseColumn(name string, nullable NullableColumn, tableName string, parent column) baseColumn {
	bc := baseColumn{
		name:      name,
		nullable:  nullable,
		tableName: tableName,
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

func (c baseColumn) serialize(statement statementType, out *queryData) error {

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
