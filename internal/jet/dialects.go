package jet

import (
	"errors"
	"strconv"
)

var ANSII = NewDialect(DialectParams{ // just for tests
	AliasQuoteChar:      '"',
	IdentifierQuoteChar: '"',
	ArgumentPlaceholder: func(ord int) string {
		return "$" + strconv.Itoa(ord)
	},
	SupportsReturning: true,
})

type Dialect interface {
	Name() string
	PackageName() string
	SerializeOverride(operator string) SerializeOverride
	AliasQuoteChar() byte
	IdentifierQuoteChar() byte
	ArgumentPlaceholder() QueryPlaceholderFunc
	SetClause() func(columns []IColumn, values []Serializer, out *SqlBuilder) (err error)
	SupportsReturning() bool
}

type SerializeFunc func(statement StatementType, out *SqlBuilder, options ...SerializeOption) error
type SerializeOverride func(expressions ...Expression) SerializeFunc

type QueryPlaceholderFunc func(ord int) string
type UpdateAssigmentFunc func(columns []IColumn, values []Serializer, out *SqlBuilder) (err error)

type DialectParams struct {
	Name                string
	PackageName         string
	SerializeOverrides  map[string]SerializeOverride
	AliasQuoteChar      byte
	IdentifierQuoteChar byte
	ArgumentPlaceholder QueryPlaceholderFunc
	SetClause           func(columns []IColumn, values []Serializer, out *SqlBuilder) (err error)

	SupportsReturning bool
}

func NewDialect(params DialectParams) Dialect {
	return &dialectImpl{
		name:                params.Name,
		packageName:         params.PackageName,
		serializeOverrides:  params.SerializeOverrides,
		aliasQuoteChar:      params.AliasQuoteChar,
		identifierQuoteChar: params.IdentifierQuoteChar,
		argumentPlaceholder: params.ArgumentPlaceholder,
		setClause:           params.SetClause,
		supportsReturning:   params.SupportsReturning,
	}
}

type dialectImpl struct {
	name                string
	packageName         string
	serializeOverrides  map[string]SerializeOverride
	aliasQuoteChar      byte
	identifierQuoteChar byte
	argumentPlaceholder QueryPlaceholderFunc
	setClause           UpdateAssigmentFunc

	supportsReturning bool
}

func (d *dialectImpl) Name() string {
	return d.name
}

func (d *dialectImpl) PackageName() string {
	return d.packageName
}

func (d *dialectImpl) SerializeOverride(operator string) SerializeOverride {
	return d.serializeOverrides[operator]
}

func (d *dialectImpl) AliasQuoteChar() byte {
	return d.aliasQuoteChar
}

func (d *dialectImpl) IdentifierQuoteChar() byte {
	return d.identifierQuoteChar
}

func (d *dialectImpl) ArgumentPlaceholder() QueryPlaceholderFunc {
	return d.argumentPlaceholder
}

func (d *dialectImpl) SetClause() func(columns []IColumn, values []Serializer, out *SqlBuilder) (err error) {
	if d.setClause != nil {
		return d.setClause
	}
	return setClause
}

func (d *dialectImpl) SupportsReturning() bool {
	return d.supportsReturning
}

func setClause(columns []IColumn, values []Serializer, out *SqlBuilder) (err error) {

	if len(columns) != len(values) {
		return errors.New("jet: mismatch in numers of columns and values")
	}

	for i, column := range columns {
		if i > 0 {
			out.WriteString(", ")
		}

		if column == nil {
			return errors.New("jet: nil column in columns list")
		}

		out.WriteString(column.Name())

		out.WriteString(" = ")

		if err = Serialize(values[i], UpdateStatementType, out); err != nil {
			return err
		}
	}

	return nil
}
